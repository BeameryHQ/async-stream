package jobs

import (
	"context"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/SeedJobs/async-stream/lb"
	"github.com/SeedJobs/async-stream/stream"
	"github.com/SeedJobs/async-stream/logging"
	"go.etcd.io/etcd/clientv3"
	"k8s.io/client-go/util/workqueue"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	defaultMaxRunningWithNoUpdate = time.Minute * 1
	defaultRetryTime              = time.Second * 5
	defaultConcurrency            = 10
)

type StreamConsumerConfiguration struct {
	Path         string `validate:"required" yaml:"Path"`
	ConsumerName string `validate:"required" yaml:"ConsumerName"`
	Concurrency  int    `validate:"required" yaml:"Concurrency"`
	// optional defaults to 5ecs
	NextRetry       time.Duration `yaml:"NextRetry"`
	RunningNoUpdate time.Duration `yaml:"RunningNoUpdate"`
}

type streamConsumer struct {
	logger          *logrus.Entry
	path            string
	consumerID      string
	queue           workqueue.RateLimitingInterface
	taskHandlers    map[string]JobHandler
	jobStore        JobStore
	ctx             context.Context
	jobFilter       lb.KeyLbNotifier
	streamCache     map[string]bool
	mu              *sync.Mutex
	flow            stream.Flow
	retryTime       time.Duration
	runningNoUpdate time.Duration
	concurrency     int
}

func NewEtcdStreamConsumer(ctx context.Context, cli *clientv3.Client, config *StreamConsumerConfiguration) (*streamConsumer, error) {
	applyDefaults(config)

	jobFilter, err := lb.NewEtcdLoadBalancer(ctx, cli, config.Path, config.ConsumerName)
	if err != nil {
		return nil, err
	}

	jobStore := NewJobStore(cli, config.Path, config.ConsumerName, config.RunningNoUpdate)
	flow := stream.NewEtcdFlow(cli)

	return NewStreamConsumer(ctx, config, jobStore, flow, jobFilter), nil
}

func NewStreamConsumer(ctx context.Context, config *StreamConsumerConfiguration, jobStore JobStore, flow stream.Flow, jobFilter lb.KeyLbNotifier) *streamConsumer {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	logger := logging.GetLogger().WithFields(logrus.Fields{
		"consumerName": config.ConsumerName,
		"path":         config.Path,
	})

	return &streamConsumer{
		logger:          logger,
		path:            config.Path,
		consumerID:      config.ConsumerName,
		queue:           queue,
		taskHandlers:    map[string]JobHandler{},
		jobStore:        jobStore,
		ctx:             ctx,
		jobFilter:       jobFilter,
		streamCache:     map[string]bool{},
		mu:              &sync.Mutex{},
		flow:            flow,
		retryTime:       config.NextRetry,
		runningNoUpdate: config.RunningNoUpdate,
		concurrency:     config.Concurrency,
	}
}

func applyDefaults(config *StreamConsumerConfiguration) {
	if config.Concurrency == 0 {
		config.Concurrency = defaultConcurrency
	}

	if config.NextRetry == 0 {
		config.NextRetry = defaultRetryTime
	}

	if config.RunningNoUpdate == 0 {
		config.RunningNoUpdate = defaultMaxRunningWithNoUpdate
	}
}

func (s *streamConsumer) Start(block bool) {
	go func() {
		s.logger.Info("starting the stream flow ")
		s.flow.RegisterListHandler(s.path, s.streamHandler)
		s.flow.RegisterWatchHandler(s.path, s.streamHandler)
		s.flow.Run(s.ctx)
	}()

	go func() {
		ch := s.jobFilter.Notify()
		for {
			select {
			case <-ch:
				s.logger.Info("lb change need to re-shuffle")
				s.requeueJobsOnLbChange()
			case <-s.ctx.Done():
				return
			}
		}
	}()

	// this goroutine monitors the workers if they fail so can restart them
	go s.runMonitorWorkers()

	waitShutdown := func() {
		select {
		case <-s.ctx.Done():
			s.queue.ShutDown()
		}
	}

	if block {
		waitShutdown()
	} else {
		go waitShutdown()
	}

}

func (s *streamConsumer) runMonitorWorkers() {
	workers := make(chan bool, s.concurrency)
	for i := 0; i < int(s.concurrency); i++ {
		workers <- true
	}

	id := 0
	for range workers {
		if s.ctx.Err() != nil {
			return
		}

		id++
		s.logger.Debugln("starting worker : ", id)
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					s.logger.Error("worker crashed restarting it ", rec)
				}
				workers <- true
			}()

			for ; ; {
				s.processQueue()
				select {
				case <-s.ctx.Done():
					return
				default:

				}
			}
		}()
	}
}

func (s *streamConsumer) RegisterHandler(taskName string, handler JobHandler) {
	s.taskHandlers[taskName] = handler
}

func (s *streamConsumer) requeueJobsOnLbChange() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for jobId := range s.streamCache {
		if !s.isJobMine(jobId) {
			continue
		}

		s.queue.Add(jobId)
	}
}

func (s *streamConsumer) isJobMine(jobId string) bool {
	target, err := s.jobFilter.Target(jobId, true)
	if err != nil {
		return false
	}

	return target == s.consumerID

}

func (s *streamConsumer) streamHandler(event *stream.FlowEvent) error {

	jobId, err := ExtractJobIdFromKey(s.path, event.Kv)
	if err != nil {
		s.logger.Warningf("the jobId extraction failed : %v ", err)
		return nil
	}

	s.mu.Lock()
	if event.IsCreated() || event.IsUpdated() {
		s.streamCache[jobId] = true
	} else {
		// update the cache on key deletion on server side
		delete(s.streamCache, jobId)
	}
	s.mu.Unlock()

	if !s.isJobMine(jobId) {
		s.logger.Debugln("this job is not mine skipping ", jobId, s.consumerID)
		return nil
	}

	if event.IsCreated() || event.IsUpdated() {
		s.queue.Add(jobId)
		s.logger.Debug("consumer.streamHandler adding job to the queue : ", jobId)
	}

	return nil
}

func (s *streamConsumer) processQueue() {
	key, exit := s.queue.Get()
	defer s.queue.Done(key)
	if exit {
		return
	}
	s.logger.Debug("processing the key : ", key)

	jobId := key.(string)
	j, err := s.jobStore.Get(s.ctx, jobId)
	if err != nil {
		s.logger.Warningf("fetching the jobId %s failed : %v ", jobId, err)
		return
	}

	// check if need to enqueue the job or is something we need to skip
	if s.shouldSkipTheJob(jobId, j) {
		return
	}

	handler := s.taskHandlers[j.TaskName]
	if handler == nil {
		s.logger.Warningf("no handler was found for task : %s", j.TaskName)
		return
	}

	//try to set the state of job to running
	//it's a small guarantee we need in case of worker shuffling
	if err := s.jobStore.MarkRunning(s.ctx, jobId); err != nil {
		s.logger.Warningf("marking the job as running failed %s : %v", jobId, err)
		if err == ErrConcurrentJobUpdate {
			now := time.Now().UTC()
			nextRun := now.Add(s.runningNoUpdate)
			runIn := nextRun.Sub(now)
			s.logger.Infof("setting up the job %s to be run in : %v", jobId, runIn)
			s.queue.AddAfter(key, runIn)
			return
		}
		s.queue.Forget(key)
		return
	}

	// make sure we forger the key so it doesn't get scheduled again
	defer s.queue.Forget(key)

	handlerLog := s.logger.WithFields(logrus.Fields{
		"taskName": j.TaskName,
		"jobId":    j.Id,
	})
	err = handler.Handle(s.jobStore, j, handlerLog)
	if err == nil {
		s.logger.Debug("the job processed successfully : ", jobId)
		if err := s.jobStore.MarkFinished(s.ctx, jobId); err != nil {
			s.logger.Errorf("marking the task as successful failed : %v", err)
		}

		return
	}
	if mErr := s.jobStore.MarkFailed(s.ctx, jobId, err); mErr != nil {
		s.logger.Errorf("marking job failed %s : %v", jobId, mErr)
	}

	return

}

func (s *streamConsumer) shouldSkipTheJob(jobId string, job *Job) bool {
	if job.State == StateFinished {
		s.logger.Debugln("the job is finished skipping processing ", job.Id)
		return true
	}

	if job.State == StateRunning {
		if job.UpdatedAt.Add(s.runningNoUpdate).After(time.Now().UTC()) {
			runIn := job.UpdatedAt.Add(s.runningNoUpdate).Sub(time.Now().UTC())
			if runIn <= 0 {
				return false
			}
			s.logger.Info("the job was marked running we need to wait a little bit :", runIn, jobId)
			s.queue.AddAfter(jobId, runIn)
			return true
		}
	}

	if job.State == StateErrored {
		if job.CurrentRetry >= job.MaxRetry {
			s.logger.Info("the job was retried max times skipping", job.Id)
			return true
		}

		retryExp := math.Pow(2, float64(job.CurrentRetry))
		nextDuration := time.Duration(retryExp) * s.retryTime
		// if it'a not the next thing we should check is the last updated time
		if job.UpdatedAt.Add(nextDuration).After(time.Now().UTC()) {
			runIn := job.UpdatedAt.Add(nextDuration).Sub(time.Now().UTC())
			if runIn < 0 {
				return false
			}
			s.logger.Debug("the job was errored we need to wait a little bit :", runIn, jobId, job.CurrentRetry)
			s.queue.AddAfter(jobId, runIn)
			return true
		}
	}

	return false
}

func ExtractJobIdFromKey(path string, key *stream.FlowKeyValue) (string, error) {

	pathPrefix := strings.TrimRight(path, "/") + "/"
	regexStr := pathPrefix + `(.+)/data`
	r, err := regexp.Compile(regexStr)
	if err != nil {
		return "", err
	}

	parts := r.FindStringSubmatch(strings.TrimRight(string(key.Key), "/"))
	if len(parts) != 2 {
		return "", fmt.Errorf("job id not found in the path : %s", string(key.Key))
	}

	return parts[1], nil
}
// +build integration

package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/SeedJobs/async-stream/stream"
	"github.com/SeedJobs/async-stream/tests"
	"go.etcd.io/etcd/clientv3"
	"os"
	"sync"
	"testing"
	"time"
)

type successHandler struct{}

func (sh *successHandler) Handle(jobStore JobStore, j *Job, logger *logrus.Entry) error {
	logger.Println("processed successfully")
	return nil
}

type errorHandler struct{}

func (sh *errorHandler) Handle(jobStore JobStore, j *Job, logger *logrus.Entry) error {
	return fmt.Errorf("always fail")
}

type panicHandler struct{}

func (sh *panicHandler) Handle(jobStore JobStore, j *Job, logger *logrus.Entry) error {
	if j.Result == "" {
		if err := jobStore.SaveResult(context.Background(), j.Id, "1"); err != nil {
			return err
		}
		panic("panic")
	}

	return fmt.Errorf("paniced once")
}

type failOnceThenOkHandler struct{}

func (sh *failOnceThenOkHandler) Handle(jobStore JobStore, j *Job, logger *logrus.Entry) error {
	if j.Result == "" {
		if err := jobStore.SaveResult(context.Background(), j.Id, `{}`); err != nil {
			return err
		}

		return fmt.Errorf("once")
	}

	return nil
}

func TestEtcdJobStore_Run(t *testing.T) {
	t.Logf("testing a successful case")

	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	consumerName := "consumer-0"

	conf := &StreamConsumerConfiguration{
		Path:            path,
		ConsumerName:    consumerName,
		NextRetry:       time.Second,
		RunningNoUpdate: time.Second * 5,
	}
	consumer, err := NewEtcdStreamConsumer(ctx, res.Cli, conf)
	if err != nil {
		t.Fatalf("creating the consumer")
	}

	taskNameSuccess := "test.finished"
	consumer.RegisterHandler(taskNameSuccess, &successHandler{})

	taskNameError := "test.error"
	consumer.RegisterHandler(taskNameError, &errorHandler{})

	taskNamePanic := "test.panic"
	consumer.RegisterHandler(taskNamePanic, &panicHandler{})

	taskNameFailOnce := "test.failOnce"
	consumer.RegisterHandler(taskNameFailOnce, &failOnceThenOkHandler{})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		consumer.Start(true)
	}()

	p := NewJobProducer(res.Cli, path)

	runUntil := &flowUntil{
		cli:     res.Cli,
		ctx:     ctx,
		path:    path,
		timeout: time.Second * 30,
	}

	testCases := []struct {
		name      string
		taskName  string
		predicate func(j *Job) (bool, error)
	}{
		{
			name:     "success",
			taskName: taskNameSuccess,
			predicate: func(j *Job) (bool, error) {
				ok := j.State == StateFinished
				return ok, nil
			}},
		{
			name:     "error",
			taskName: taskNameError,
			predicate: func(j *Job) (bool, error) {
				ok := j.State == StateErrored
				if !ok {
					return false, nil
				}

				if j.CurrentRetry < j.MaxRetry {
					return false, nil
				}

				if j.Errors[len(j.Errors)-1] != "always fail" {
					return false, fmt.Errorf("err mismatch ")
				}

				return true, nil
			}},

		{
			name:     "panic",
			taskName: taskNamePanic,
			predicate: func(j *Job) (bool, error) {
				ok := j.State == StateErrored
				if !ok {
					return false, nil
				}

				if j.CurrentRetry < j.MaxRetry {
					return false, nil
				}

				if j.Errors[len(j.Errors)-1] != "paniced once" {
					return false, fmt.Errorf("err mismatch ")
				}

				return true, nil
			}},
		{
			name:     "failonce",
			taskName: taskNameFailOnce,
			predicate: func(j *Job) (bool, error) {
				ok := j.State == StateFinished
				if !ok {
					return false, nil
				}

				if j.Errors[len(j.Errors)-1] != "once" {
					return false, fmt.Errorf("err mismatch ")
				}

				return true, nil
			}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			producerJobId, err := p.SubmitWithRetry(ctx, tc.taskName, JobParameters{}, 2)
			if err != nil {
				tt.Fatalf("submitting job failed : %v", err)
			}

			err = runUntil.runForJob(producerJobId, tc.predicate)
			if err != nil {
				tt.Fatalf("runUntil failed for : %s : %v", producerJobId, err)
			}
		})
	}

	cancel()
	t.Logf("waiting for consumer to exit ")
	wg.Wait()
}

type flowUntil struct {
	cli     *clientv3.Client
	ctx     context.Context
	path    string
	timeout time.Duration
}

func (fu *flowUntil) runForJob(jobId string, cb func(job *Job) (bool, error)) error {
	jobIdwrapper := func(jobId string) func(job *Job) (bool, error) {
		return func(job *Job) (bool, error) {
			if job.Id != jobId {
				return false, nil
			}

			return cb(job)
		}
	}

	return RunJobFlowUntil(
		fu.ctx,
		fu.cli,
		fu.path,
		jobIdwrapper(jobId),
		fu.timeout,
	)
}

// interesting idea that can be used for some other cases, when need to process the stream till a certain point
func RunJobFlowUntil(ctx context.Context, cli *clientv3.Client, path string, cb func(job *Job) (bool, error), timeout time.Duration) error {
	// ctx to work on for child paths
	ctx, cancel := context.WithCancel(ctx)

	upstreamChan := make(chan bool)
	errorUpstream := make(chan error)

	flowHandler := func(event *stream.FlowEvent) error {
		//fmt.Printf("got event %+v \n", event)
		_, err := ExtractJobIdFromKey(path, event.Kv)
		if err != nil {
			//t.Logf("wasn't able to parse job id : %v", err)
			return nil
		}
		//fmt.Printf("got event for job id %s - %s\n", jobId, event.Kv.Value)
		var j Job

		if err := json.Unmarshal([]byte(event.Kv.Value), &j); err != nil {
			return err
		}

		ok, err := cb(&j)
		if err != nil {
			//fmt.Println("sending the error upstream ", err)
			errorUpstream <- err
			return nil
		}

		if ok {
			upstreamChan <- true
		}

		return nil

	}

	f := stream.NewEtcdFlow(cli)
	f.RegisterWatchHandler(path, flowHandler)
	f.RegisterListHandler(path, flowHandler)

	go func() {
		f.Run(ctx)
	}()

	//fmt.Println("waiting for something to happen ...")

	defer func() {
		if ctx.Err() == nil {
			cancel()
		}
	}()

	select {
	case <-upstreamChan:
		//fmt.Println("got the predicate success")
		return nil
	case err := <-errorUpstream:
		//fmt.Println("got error upstream exiting ", err)
		return err
	case <-time.After(timeout):
		//fmt.Println("exiting due timeout")
		return fmt.Errorf("timeout")
	case <-ctx.Done():
		//fmt.Println("exiting due ctx ", ctx.Err())
		return ctx.Err()
	}

}

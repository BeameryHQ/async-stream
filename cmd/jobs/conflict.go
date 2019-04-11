package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/BeameryHQ/async-stream/jobs"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/fatih/color"
	"go.etcd.io/etcd/clientv3"
	"log"
	"sync"
	"time"
)

var (
	path            = flag.String("p", "", "path to monitor")
	verbose         = flag.Bool("v", false, "print whatever happens")
	noUpdateRunning = flag.Int("r", 60, "the period after a worker can steal a job if not activity (secs)")

	jobCache = map[string]*jobs.Job{}
	mu       sync.Mutex
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatalf("etcd client creation failed %v", err)
	}

	defer cli.Close()
	defer cancel()

	jobsPath := *path

	flow := stream.NewEtcdFlow(cli)
	flow.RegisterListHandler(jobsPath, streamHandler(jobsPath))
	flow.RegisterWatchHandler(jobsPath, streamHandler(jobsPath))
	flow.Run(ctx)
}

func streamHandler(jobsPath string) func(event *stream.FlowEvent) error {
	return func(event *stream.FlowEvent) error {
		mu.Lock()
		defer mu.Unlock()

		extractedJobId, err := jobs.ExtractJobIdFromKey(jobsPath, event.Kv)
		if err != nil {
			return nil
		}

		if event.IsDeleted() {
			if *verbose {
				color.White("Job : %s was deleted", extractedJobId)
			}
			delete(jobCache, extractedJobId)
			return nil
		}

		j := &jobs.Job{}
		err = json.Unmarshal([]byte(event.Kv.Value), j)
		if err != nil {
			return err
		}

		if event.IsCreated() {
			if *verbose {
				color.White("Job: %s was created", extractedJobId)
			}
			jobCache[extractedJobId] = j
			return nil
		}

		oldJob := jobCache[extractedJobId]
		if oldJob == nil {
			if *verbose {
				color.White("Job: %s was updated (not in cache skip validate)", extractedJobId)
			}
			jobCache[extractedJobId] = j
			return nil
		}

		validateJob(oldJob, j)
		jobCache[extractedJobId] = j

		return nil
	}
}

func validateJob(oldJob, newJob *jobs.Job) {
	isTouch, err := isItTouched(oldJob, newJob)
	if err != nil {
		color.Red("touch control failed for %s : %v", newJob.Id, err)
		return
	}

	if *verbose {
		if !isTouch {
			color.Magenta("Job %s was updated to %s by %s", newJob.Id, newJob.State, newJob.ConsumerName)
		} else {
			color.Magenta("Job %s was touched by %s", newJob.Id, newJob.ConsumerName)

		}
	}

	if err := validateConsumerChange(oldJob, newJob); err != nil {
		color.Red("Job : %s consumer change validation failed : %v", newJob.Id, err)
	}

	if isTouch {
		if err := validateTouchChange(oldJob, newJob); err != nil {
			color.Red("Job : %s touch update failed : %v", newJob.Id, err)
		}
	}

	if err := validateStateTransition(oldJob, newJob); err != nil {
		color.Red("Job: %s state transition failure : %v", newJob.Id, err)
	}

}

func validateConsumerChange(oldJob, newJob *jobs.Job) error {
	if oldJob.ConsumerName == newJob.ConsumerName {
		return nil
	}

	if oldJob.State == "" && newJob.State == jobs.StateRunning {
		return nil
	}

	if oldJob.State == jobs.StateRunning && newJob.State == jobs.StateRunning {
		oldUpdate := oldJob.UpdatedAt
		newUpdate := newJob.UpdatedAt

		diff := newUpdate.Sub(oldUpdate)
		treshold := time.Duration(time.Second * time.Duration(*noUpdateRunning))

		if diff < treshold {
			return fmt.Errorf("consumer %s stole from %s before treshold at :%s", oldJob.ConsumerName, newJob.ConsumerName, diff)
		}
	}

	return nil
}

func validateTouchChange(oldJob, newJob *jobs.Job) error {
	oldState := oldJob.State

	if oldState == jobs.StateErrored {
		return fmt.Errorf("invalid touch from state errored ")
	}

	if oldState == jobs.StateFinished {
		return fmt.Errorf("invalid touch from state finished ")
	}

	return nil
}

func validateStateTransition(oldJob, newJob *jobs.Job) error {
	validTransitions := [][]string{
		{"", jobs.StateRunning},
		{jobs.StateRunning, jobs.StateFinished},
		{jobs.StateRunning, jobs.StateRunning},
		{jobs.StateRunning, jobs.StateErrored},
		{jobs.StateErrored, jobs.StateRunning},
	}

	oldState := oldJob.State
	newState := newJob.State

	found := false
	for _, t := range validTransitions {
		if t[0] == oldState && t[1] == newState {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("invalid state transition from %s to %s", oldState, newState)
	}

	return nil
}

func isItTouched(oldJob, newJob *jobs.Job) (bool, error) {
	if oldJob.State != newJob.State {
		return false, nil
	}

	if oldJob.ConsumerName != newJob.ConsumerName {
		return false, nil
	}

	if oldJob.Result != newJob.Result {
		return false, nil
	}

	if len(oldJob.Errors) != len(newJob.Errors) {
		return false, nil
	}

	return true, nil
}

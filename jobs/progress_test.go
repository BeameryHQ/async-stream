package jobs

import (
	"context"
	"fmt"
	kvstore2 "github.com/BeameryHQ/async-stream/kvstore"
	"github.com/BeameryHQ/async-stream/tests"
	"github.com/Sirupsen/logrus"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProgress_RunWithProgressResult(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, time.Second*2)
	producer := NewProducer(kvstore, path)

	messages := []string{"one", "two", "three"}
	alreadyProcessed := map[string]bool{}

	var recvMessagesIndex int32
	predicate := func(j *Job) (bool, error) {
		if j.State != StateRunning {
			return false, fmt.Errorf("state should be runnnig")
		}

		expectedRes := messages[recvMessagesIndex]
		gotRes := strings.Trim(j.Result, `"`)

		if gotRes == "initial" {
			return false, nil
		}

		if expectedRes != gotRes {
			if alreadyProcessed[gotRes] {
				t.Logf("some duplicate data arrived, can happen")
				return false, nil
			}
			return false, fmt.Errorf("expected result : %s got : %s", expectedRes, gotRes)
		}

		alreadyProcessed[gotRes] = true
		atomic.AddInt32(&recvMessagesIndex, 1)
		numOfProcessed := atomic.LoadInt32(&recvMessagesIndex)

		if int(numOfProcessed) != len(messages) {
			return false, nil
		}
		return true, nil
	}

	j := &Job{
		Id:           "running-result",
		TaskName:     "running",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
		MaxRetry:     2,
		CurrentRetry: 1,
		State:        StateRunning,
		Result:       "initial",
		Errors: []string{
			"duplicate",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := producer.SubmitRaw(ctx, j)
	if err != nil {
		t.Fatalf("submitting the job failed %v", err)
	}

	progr := NewProgress(jobStore, logrus.WithField("app", "progress"))

	runUntil := &flowUntil{
		cli:     res.Cli,
		ctx:     ctx,
		path:    path,
		timeout: time.Second * 30,
	}

	// send some progress data
	go func() {
		err := progr.RunWithProgressResult(ctx, j.Id, func(progressChan chan<- interface{}) error {
			for _, m := range messages {
				progressChan <- m
				time.Sleep(time.Second)
			}

			return nil
		})

		if err != nil {
			cancel()
			t.Fatalf("running with progress failed : %v", err)
		}
	}()

	err = runUntil.runForJob(j.Id, predicate)
	if err != nil {
		t.Fatalf("runUntil failed for : %s : %v", j.Id, err)
	}
	cancel()

}

func TestProgress_RunWithProgressTime(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, time.Second*2)
	producer := NewProducer(kvstore, path)

	minNumOfUpdates := 4

	now := time.Now().UTC()
	j := &Job{
		Id:           "running-result",
		TaskName:     "running",
		CreatedAt:    now,
		UpdatedAt:    now,
		MaxRetry:     2,
		CurrentRetry: 1,
		State:        StateRunning,
		Result:       "initial",
		Errors: []string{
			"duplicate",
		},
	}

	var mu sync.Mutex
	lastTimeUpdate := j.UpdatedAt

	var recvMessagesIndex int32
	predicate := func(j *Job) (bool, error) {
		if j.State != StateRunning {
			return false, fmt.Errorf("state should be runnnig")
		}

		gotRes := strings.Trim(j.Result, `"`)
		if gotRes != "initial" {
			return false, fmt.Errorf("the result changed should've stayed same %s", gotRes)
		}

		// means havent got any messages yet
		if j.CreatedAt == j.UpdatedAt {
			t.Logf("created equals updated wait next clock")
			return false, nil
		}

		atomic.AddInt32(&recvMessagesIndex, 1)
		numOfProcessed := atomic.LoadInt32(&recvMessagesIndex)

		if !j.UpdatedAt.After(lastTimeUpdate) {
			return false, fmt.Errorf("updated at should be : %v, after : %v", j.UpdatedAt, lastTimeUpdate)
		}

		mu.Lock()
		defer mu.Unlock()
		lastTimeUpdate = j.UpdatedAt

		if int(numOfProcessed) < minNumOfUpdates {
			return false, nil
		}
		return true, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := producer.SubmitRaw(ctx, j)
	if err != nil {
		t.Fatalf("submitting the job failed %v", err)
	}

	progr := NewProgress(jobStore, logrus.WithField("app", "progress"))

	runUntil := &flowUntil{
		cli:     res.Cli,
		ctx:     ctx,
		path:    path,
		timeout: time.Second * 30,
	}

	// send some progress data
	go func() {
		// run for 4 secs
		err := progr.RunWithProgressTime(ctx, j.Id, func() error {
			time.Sleep(time.Second * 10)
			return nil
		}, WithTickTime(time.Millisecond*200))

		if err != nil {
			cancel()
			t.Fatalf("running with progress failed : %v", err)
		}
	}()

	err = runUntil.runForJob(j.Id, predicate)
	if err != nil {
		t.Fatalf("runUntil failed for : %s : %v", j.Id, err)
	}
	cancel()

}

func TestProgress_RunWithProgressTimeWithProgressResult(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, time.Second*2)
	producer := NewProducer(kvstore, path)

	messages := []string{"one", "two", "three"}

	var recvMessagesIndex int32
	predicate := func(j *Job) (bool, error) {
		if j.State != StateRunning {
			return false, fmt.Errorf("state should be runnnig")
		}

		expectedRes := messages[recvMessagesIndex]
		gotRes := strings.Trim(j.Result, `"`)

		if gotRes == "initial" {
			return false, nil
		}

		if expectedRes != gotRes {
			numOfProcessed := atomic.LoadInt32(&recvMessagesIndex)
			if numOfProcessed > 0 {
				prevRes := messages[numOfProcessed-1]
				if prevRes == gotRes {
					return false, nil
				} else {
					return false, fmt.Errorf("even prev message didn't mathc %s", prevRes)
				}
			}
			return false, fmt.Errorf("expected result : %s got : %s", expectedRes, gotRes)
		}
		atomic.AddInt32(&recvMessagesIndex, 1)
		numOfProcessed := atomic.LoadInt32(&recvMessagesIndex)

		if int(numOfProcessed) != len(messages) {
			return false, nil
		}
		return true, nil
	}

	j := &Job{
		Id:           "running-result",
		TaskName:     "running",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
		MaxRetry:     2,
		CurrentRetry: 1,
		State:        StateRunning,
		Result:       "initial",
		Errors: []string{
			"duplicate",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := producer.SubmitRaw(ctx, j)
	if err != nil {
		t.Fatalf("submitting the job failed %v", err)
	}

	progr := NewProgress(jobStore, logrus.WithField("app", "progress"))

	runUntil := &flowUntil{
		cli:     res.Cli,
		ctx:     ctx,
		path:    path,
		timeout: time.Second * 30,
	}

	// send some progress data
	go func() {

		// this is how things will be running inside of the consumer
		err := progr.RunWithProgressTime(ctx, j.Id, func() error {
			return progr.RunWithProgressResult(ctx, j.Id, func(progressChan chan<- interface{}) error {
				for _, m := range messages {
					progressChan <- m
					time.Sleep(time.Second * 2)
				}
				return nil

			})
		}, WithTickTime(time.Second))
		if err != nil {
			cancel()
			t.Fatalf("running with progress failed : %v", err)
		}
	}()

	err = runUntil.runForJob(j.Id, predicate)
	if err != nil {
		t.Fatalf("runUntil failed for : %s : %v", j.Id, err)
	}
	cancel()

}

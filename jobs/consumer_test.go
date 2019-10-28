package jobs

import (
	"context"
	"fmt"
	"github.com/BeameryHQ/async-stream/tests"
	"github.com/sirupsen/logrus"
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

type alwaysPanicHandler struct{}

func (sh *alwaysPanicHandler) Handle(jobStore JobStore, j *Job, logger *logrus.Entry) error {
	panic("panic")
}

func TestStreamConsumer_StartLbStop(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	ctx := context.Background()
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

	taskNameAlwaysPanic := "test.alwaysPanic"
	consumer.RegisterHandler(taskNameAlwaysPanic, &alwaysPanicHandler{})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		consumer.Start(true)
	}()

	consumer.cancel()
	wg.Wait()

}

func TestEtcdJobStore_Run(t *testing.T) {
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

	taskNameAlwaysPanic := "test.alwaysPanic"
	consumer.RegisterHandler(taskNameAlwaysPanic, &alwaysPanicHandler{})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		consumer.Start(true)
	}()

	p := NewEtcdJobProducer(res.Cli, path)

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
		{
			name:     "alwayspanic",
			taskName: taskNameAlwaysPanic,
			predicate: func(j *Job) (bool, error) {
				ok := j.State == StateErrored
				if !ok {
					return false, nil
				}

				if j.CurrentRetry < j.MaxRetry {
					return false, nil
				}

				if j.Errors[len(j.Errors)-1] != "panic" {
					return false, fmt.Errorf("err mismatch %s", j.Errors[len(j.Errors)-1])
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

package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	kvstore2 "github.com/BeameryHQ/async-stream/kvstore"
	"github.com/BeameryHQ/async-stream/tests"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"time"
)

func TestJobStore_MarkRunning(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, 0)
	producer := NewProducer(kvstore, path)

	ctx := context.Background()

	testCases := []struct {
		name         string
		job          *Job
		waitTime     time.Duration
		err          error
		errContaints string
	}{
		{
			name: "mark running created",
			job: &Job{
				Id:        "created",
				TaskName:  "running",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				MaxRetry:  1,
			},
		},
		{
			name: "fail mark running already running",
			job: &Job{
				Id:        "runningconflict",
				TaskName:  "running",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				MaxRetry:  1,
				State:     StateRunning,
			},
			err: kvstore2.ErrConcurrentUpdate,
		},
		{
			name: "mark running already running",
			job: &Job{
				Id:        "runningok",
				TaskName:  "running",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				MaxRetry:  1,
				State:     StateRunning,
			},
			waitTime: time.Second * 3,
		},
		{
			name: "mark running errored",
			job: &Job{
				Id:        "runningerrored",
				TaskName:  "running",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				MaxRetry:  1,
				State:     StateErrored,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			if err := producer.SubmitRaw(ctx, tc.job); err != nil {
				tt.Fatalf("submit failed : %v", err)
			}

			if tc.waitTime != 0 {
				time.Sleep(tc.waitTime)
			}

			runnigErr := jobStore.MarkRunning(ctx, tc.job.Id)
			if tc.err == nil && tc.errContaints == "" && runnigErr != nil {
				tt.Fatalf("expected success got : %v", runnigErr)
			}

			if tc.err != nil && tc.err != runnigErr {
				tt.Fatalf("error mismatch expected: %v, got: %v", tc.err, runnigErr)
			}

			if tc.errContaints != "" && (runnigErr == nil || !strings.Contains(strings.ToLower(runnigErr.Error()), strings.ToLower(tc.errContaints))) {
				tt.Fatalf("expected runnigErr str to contain : %s but got : %v", tc.errContaints, runnigErr)
			}

			currentJob, err := jobStore.Get(ctx, tc.job.Id)
			if err != nil {
				tt.Fatalf("job retrieval failed : %v", runnigErr)
			}

			if runnigErr == nil {
				//check if the job was marked as running
				assert.Equal(tt, tc.job.Id, currentJob.Id)
				assert.Equal(tt, StateRunning, currentJob.State)
				assert.Equal(tt, tc.job.TaskName, currentJob.TaskName)
				assert.Equal(tt, tc.job.MaxRetry, currentJob.MaxRetry)
				assert.Equal(tt, tc.job.CurrentRetry, currentJob.CurrentRetry)
				assert.Equal(tt, tc.job.CreatedAt, currentJob.CreatedAt)
				assert.True(tt, currentJob.UpdatedAt.After(tc.job.UpdatedAt))
			} else {
				// there should be no change in the job itself
				assert.Equal(tt, tc.job, currentJob)
			}
		})

	}
}

func TestJobStore_Touch(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, 0)
	producer := NewProducer(kvstore, path)

	ctx := context.Background()

	testCases := []struct {
		name         string
		job          *Job
		waitTime     time.Duration
		err          error
		errContaints string
	}{
		{
			name: "touch already running",
			job: &Job{
				Id:        "touch",
				TaskName:  "running",
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				MaxRetry:  1,
				State:     StateRunning,
				Result:    "somevalue",
				Errors: []string{
					"duplicate",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			if err := producer.SubmitRaw(ctx, tc.job); err != nil {
				tt.Fatalf("submit failed : %v", err)
			}

			if tc.waitTime != 0 {
				time.Sleep(tc.waitTime)
			}

			runnigErr := jobStore.Touch(ctx, tc.job.Id)
			if tc.err == nil && tc.errContaints == "" && runnigErr != nil {
				tt.Fatalf("expected success got : %v", runnigErr)
			}

			if tc.err != nil && tc.err != runnigErr {
				tt.Fatalf("error mismatch expected: %v, got: %v", tc.err, runnigErr)
			}

			if tc.errContaints != "" && (runnigErr == nil || !strings.Contains(strings.ToLower(runnigErr.Error()), strings.ToLower(tc.errContaints))) {
				tt.Fatalf("expected runnigErr str to contain : %s but got : %v", tc.errContaints, runnigErr)
			}

			currentJob, err := jobStore.Get(ctx, tc.job.Id)
			if err != nil {
				tt.Fatalf("job retrieval failed : %v", runnigErr)
			}

			if runnigErr == nil {
				//check if the job was marked as running
				assert.Equal(tt, tc.job.Id, currentJob.Id)
				assert.Equal(tt, tc.job.State, currentJob.State)
				assert.Equal(tt, tc.job.TaskName, currentJob.TaskName)
				assert.Equal(tt, tc.job.MaxRetry, currentJob.MaxRetry)
				assert.Equal(tt, tc.job.CurrentRetry, currentJob.CurrentRetry)
				assert.Equal(tt, tc.job.CreatedAt, currentJob.CreatedAt)
				assert.Equal(tt, tc.job.Errors, currentJob.Errors)
				assert.True(tt, currentJob.UpdatedAt.After(tc.job.UpdatedAt))
			} else {
				// there should be no change in the job itself
				assert.Equal(tt, tc.job, currentJob)
			}
		})

	}

}

func TestJobStore_MarkFinished(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, time.Second*2)
	producer := NewProducer(kvstore, path)

	ctx := context.Background()

	testCases := []struct {
		name string
		job  *Job
		// wait for the retention time
		retentionWaitTime time.Duration
		err               error
		errContaints      string
	}{
		{
			name: "mark finished running",
			job: &Job{
				Id:           "finshed",
				TaskName:     "running",
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
				MaxRetry:     2,
				CurrentRetry: 1,
				State:        StateRunning,
				Result:       "somevalue",
				Errors: []string{
					"duplicate",
				},
			},
			retentionWaitTime: time.Second * 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			if err := producer.SubmitRaw(ctx, tc.job); err != nil {
				tt.Fatalf("submit failed : %v", err)
			}

			runnigErr := jobStore.MarkFinished(ctx, tc.job.Id)
			if tc.err == nil && tc.errContaints == "" && runnigErr != nil {
				tt.Fatalf("expected success got : %v", runnigErr)
			}

			if tc.err != nil && tc.err != runnigErr {
				tt.Fatalf("error mismatch expected: %v, got: %v", tc.err, runnigErr)
			}

			if tc.errContaints != "" && (runnigErr == nil || !strings.Contains(strings.ToLower(runnigErr.Error()), strings.ToLower(tc.errContaints))) {
				tt.Fatalf("expected runnigErr str to contain : %s but got : %v", tc.errContaints, runnigErr)
			}

			currentJob, err := jobStore.Get(ctx, tc.job.Id)
			if err != nil {
				tt.Fatalf("job retrieval failed : %v", runnigErr)
			}

			if runnigErr == nil {
				//check if the job was marked as running
				assert.Equal(tt, tc.job.Id, currentJob.Id)
				assert.Equal(tt, StateFinished, currentJob.State)
				assert.Equal(tt, tc.job.TaskName, currentJob.TaskName)
				assert.Equal(tt, tc.job.MaxRetry, currentJob.MaxRetry)
				assert.Equal(tt, tc.job.CurrentRetry, currentJob.CurrentRetry)
				assert.Equal(tt, tc.job.CreatedAt, currentJob.CreatedAt)
				assert.Equal(tt, tc.job.Errors, currentJob.Errors)
				assert.True(tt, currentJob.UpdatedAt.After(tc.job.UpdatedAt))

				if tc.retentionWaitTime != 0 {
					time.Sleep(tc.retentionWaitTime)
				}

				// the record should be gone by this time
				_, err := jobStore.Get(ctx, tc.job.Id)
				if err == nil {
					tt.Fatalf("job retrieval should have failed")
				} else if err != kvstore2.ErrNotFound {
					tt.Fatalf("expected not found err got : %v", err)
				}
			} else {
				// there should be no change in the job itself
				assert.Equal(tt, tc.job, currentJob)
			}
		})

	}
}

func TestJobStore_MarkFailed(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, time.Second*2)
	producer := NewProducer(kvstore, path)

	ctx := context.Background()

	testCases := []struct {
		name string
		job  *Job
		// wait for the retention time
		retentionWaitTime time.Duration
		err               error
		errContaints      string
		jobError          error
	}{
		{
			name: "mark running errored",
			job: &Job{
				Id:           "erroredfirst",
				TaskName:     "running",
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
				MaxRetry:     2,
				CurrentRetry: 0,
				State:        StateRunning,
				Result:       "somevalue",
			},
			jobError: fmt.Errorf("first retry"),
		},
		{
			name: "retry errored",
			job: &Job{
				Id:           "errored",
				TaskName:     "running",
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
				MaxRetry:     3,
				CurrentRetry: 1,
				State:        StateRunning,
				Result:       "somevalue",
				Errors: []string{
					"duplicate",
				},
			},
			jobError: fmt.Errorf("retry"),
		},
		{
			name: "mark running failed",
			job: &Job{
				Id:           "failed",
				TaskName:     "running",
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
				MaxRetry:     2,
				CurrentRetry: 1,
				State:        StateRunning,
				Result:       "somevalue",
				Errors: []string{
					"duplicate",
				},
			},
			jobError:          fmt.Errorf("failed"),
			retentionWaitTime: time.Second * 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			if err := producer.SubmitRaw(ctx, tc.job); err != nil {
				tt.Fatalf("submit failed : %v", err)
			}

			runnigErr := jobStore.MarkFailed(ctx, tc.job.Id, tc.jobError)
			if tc.err == nil && tc.errContaints == "" && runnigErr != nil {
				tt.Fatalf("expected success got : %v", runnigErr)
			}

			if tc.err != nil && tc.err != runnigErr {
				tt.Fatalf("error mismatch expected: %v, got: %v", tc.err, runnigErr)
			}

			if tc.errContaints != "" && (runnigErr == nil || !strings.Contains(strings.ToLower(runnigErr.Error()), strings.ToLower(tc.errContaints))) {
				tt.Fatalf("expected runnigErr str to contain : %s but got : %v", tc.errContaints, runnigErr)
			}

			currentJob, err := jobStore.Get(ctx, tc.job.Id)
			if err != nil {
				tt.Fatalf("job retrieval failed : %v", runnigErr)
			}

			if runnigErr == nil {
				//check if the job was marked as running
				assert.Equal(tt, tc.job.Id, currentJob.Id)
				assert.Equal(tt, StateErrored, currentJob.State)
				assert.Equal(tt, tc.job.TaskName, currentJob.TaskName)
				assert.Equal(tt, tc.job.MaxRetry, currentJob.MaxRetry)
				assert.Equal(tt, tc.job.CurrentRetry+1, currentJob.CurrentRetry)
				assert.Equal(tt, tc.job.CreatedAt, currentJob.CreatedAt)
				assert.Equal(tt, len(tc.job.Errors)+1, len(currentJob.Errors))
				assert.Equal(tt, tc.jobError.Error(), currentJob.Errors[len(currentJob.Errors)-1])
				assert.True(tt, currentJob.UpdatedAt.After(tc.job.UpdatedAt))

				if tc.retentionWaitTime != 0 {
					time.Sleep(tc.retentionWaitTime)
				} else {
					return
				}

				// the record should be gone by this time
				_, err := jobStore.Get(ctx, tc.job.Id)
				if err == nil {
					tt.Fatalf("job retrieval should have failed")
				} else if err != kvstore2.ErrNotFound {
					tt.Fatalf("expected not found err got : %v", err)
				}
			} else {
				// there should be no change in the job itself
				assert.Equal(tt, tc.job, currentJob)
			}
		})

	}
}

func TestJobStore_SaveResult(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kvstore := kvstore2.NewEtcdKVStore(res.Cli)
	jobStore := NewJobStore(kvstore, path, "consumer-0", time.Second*2, time.Second*2)
	producer := NewProducer(kvstore, path)

	ctx := context.Background()

	testCases := []struct {
		name string
		job  *Job
		// wait for the retention time
		err          error
		errContaints string
		jobResult    map[string]string
	}{
		{
			name: "save first result running",
			job: &Job{
				Id:           "save-result",
				TaskName:     "running",
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
				MaxRetry:     2,
				CurrentRetry: 0,
				State:        StateRunning,
			},
			jobResult: map[string]string{"progress": "first"},
		},
		{
			name: "override result running",
			job: &Job{
				Id:           "override-result",
				TaskName:     "running",
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
				MaxRetry:     2,
				CurrentRetry: 0,
				State:        StateRunning,
				Result:       "old",
			},
			jobResult: map[string]string{"progress": "new"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			if err := producer.SubmitRaw(ctx, tc.job); err != nil {
				tt.Fatalf("submit failed : %v", err)
			}

			runnigErr := jobStore.SaveResult(ctx, tc.job.Id, tc.jobResult)
			if tc.err == nil && tc.errContaints == "" && runnigErr != nil {
				tt.Fatalf("expected success got : %v", runnigErr)
			}

			if tc.err != nil && tc.err != runnigErr {
				tt.Fatalf("error mismatch expected: %v, got: %v", tc.err, runnigErr)
			}

			if tc.errContaints != "" && (runnigErr == nil || !strings.Contains(strings.ToLower(runnigErr.Error()), strings.ToLower(tc.errContaints))) {
				tt.Fatalf("expected runnigErr str to contain : %s but got : %v", tc.errContaints, runnigErr)
			}

			currentJob, err := jobStore.Get(ctx, tc.job.Id)
			if err != nil {
				tt.Fatalf("job retrieval failed : %v", runnigErr)
			}

			if runnigErr == nil {
				//check if the job was marked as running
				assert.Equal(tt, tc.job.Id, currentJob.Id)
				assert.Equal(tt, StateRunning, currentJob.State)
				assert.Equal(tt, tc.job.TaskName, currentJob.TaskName)
				assert.Equal(tt, tc.job.MaxRetry, currentJob.MaxRetry)
				assert.Equal(tt, tc.job.CreatedAt, currentJob.CreatedAt)
				assert.True(tt, currentJob.UpdatedAt.After(tc.job.UpdatedAt))
				val := map[string]string{}
				err = json.Unmarshal([]byte(currentJob.Result), &val)
				if err != nil {
					tt.Fatalf("unmarshall result : %v", err)
				}

				assert.Equal(tt, tc.jobResult, val)
			} else {
				// there should be no change in the job itself
				assert.Equal(tt, tc.job, currentJob)
			}
		})
	}
}

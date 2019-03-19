package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BeameryHQ/async-stream/kvstore"
	"github.com/BeameryHQ/async-stream/metrics"
	"strings"
	"time"
)

const (
	StateFinished = "FINISHED"
	StateErrored  = "ERRORED"
	StateRunning  = "RUNNING"
)

type JobStore interface {
	Get(ctx context.Context, jobId string) (*Job, error)
	MarkFinished(ctx context.Context, jobId string) error
	MarkFailed(ctx context.Context, jobId string, err error) error
	SaveResult(ctx context.Context, jobId string, result interface{}) error
	MarkRunning(ctx context.Context, jobId string) error
}

type jobStore struct {
	cli             kvstore.Store
	path            string
	consumerName    string
	runningNoUpdate time.Duration
}

func NewJobStore(cli kvstore.Store, path string, consumerName string, runningNoUpdate time.Duration) JobStore {
	return &jobStore{
		cli:             cli,
		path:            path,
		consumerName:    consumerName,
		runningNoUpdate: runningNoUpdate,
	}
}

func (js *jobStore) Get(ctx context.Context, jobId string) (*Job, error) {
	jobKey := js.fullPath(jobId)
	kv, err := js.cli.Get(
		ctx,
		jobKey)

	if err != nil {
		return nil, err
	}

	var j Job
	err = json.Unmarshal([]byte(kv.Value), &j)
	if err != nil {
		return nil, fmt.Errorf("unmarshall job %s : %v", jobId, err)
	}

	return &j, nil
}

func (js *jobStore) MarkFinished(ctx context.Context, jobId string) error {
	jobKey := js.fullPath(jobId)
	job, err := js.Get(ctx, jobId)
	if err != nil {
		return err
	}

	job.State = StateFinished
	err = js.putJob(ctx, jobKey, job, true)
	if err == nil {
		metrics.IncrJobsFinished()
	}
	return err
}

func (js *jobStore) MarkFailed(ctx context.Context, jobId string, jobError error) error {
	jobKey := js.fullPath(jobId)
	job, err := js.Get(ctx, jobId)
	if err != nil {
		return err
	}

	job.CurrentRetry += 1
	lease := false

	if job.CurrentRetry >= job.MaxRetry {
		lease = true
	}

	job.State = StateErrored
	if job.Errors == nil {
		job.Errors = []string{}
	}
	job.Errors = append(job.Errors, jobError.Error())
	err = js.putJob(ctx, jobKey, job, lease)
	if err == nil {
		if lease {
			metrics.IncrJobsFailed()
		} else {
			metrics.IncrJobsErrored()
		}
	}
	return err
}

func (js *jobStore) SaveResult(ctx context.Context, jobId string, result interface{}) error {
	jobKey := js.fullPath(jobId)
	job, err := js.Get(ctx, jobId)
	if err != nil {
		return err
	}

	job.State = StateRunning

	jobResult, err := json.Marshal(result)
	if err != nil {
		return err
	}

	job.Result = string(jobResult)
	return js.putJob(ctx, jobKey, job, false)
}

func (js *jobStore) MarkRunning(ctx context.Context, jobId string) error {
	jobKey := js.fullPath(jobId)
	jobKV, err := js.cli.Get(ctx, jobKey)
	if err != nil {
		return nil
	}

	value := []byte(jobKV.Value)

	var j Job
	err = json.Unmarshal(value, &j)
	if err != nil {
		return fmt.Errorf("unmarshall job %s : %v", jobId, err)
	}

	// check if it's already running and if can be acquired
	now := time.Now().UTC()
	lockExpiryTime := j.UpdatedAt.Add(js.runningNoUpdate)
	if j.State == StateRunning && lockExpiryTime.After(now) {
		return kvstore.ErrConcurrentUpdate
	}

	if j.State == StateFinished {
		return fmt.Errorf("already finished nothing to do : %v", jobId)
	}

	return js.putJobTxn(ctx, jobKey, &j, jobKV.ModRevision)

}

func (js *jobStore) putJob(ctx context.Context, key string, job *Job, lease bool) error {
	// set a lease here for the stuff we're not going to update anymore
	job.UpdatedAt = time.Now().UTC()

	val, err := json.Marshal(job)
	if err != nil {
		return err
	}

	if !lease {
		err = js.cli.Put(ctx, key, string(val), kvstore.WithNoLease())
		return err
	}

	err = js.cli.Put(ctx, key, string(val))
	return err
}

func (js *jobStore) putJobTxn(ctx context.Context, key string, job *Job, modVersion int64) error {
	val, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return js.cli.Put(ctx, key, string(val), kvstore.WithVersion(modVersion))
}

func (js *jobStore) fullPath(jobId string) string {
	return strings.Join([]string{js.path, jobId, "data"}, "/")
}

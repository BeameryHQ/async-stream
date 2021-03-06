package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BeameryHQ/async-stream/kvstore"
	"github.com/BeameryHQ/async-stream/metrics"
	"github.com/BeameryHQ/async-stream/util"
	"log"
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
	// Touch only updates the UpdatedAt field of the job it doesn't affect any other fields
	Touch(ctx context.Context, jobId string) error
}

type jobStore struct {
	cli             kvstore.Store
	path            string
	consumerName    string
	runningNoUpdate time.Duration
	// if not set it'll use the default what's in the kvstore
	retentionPeriod time.Duration
}

func NewJobStore(cli kvstore.Store, path string, consumerName string, runningNoUpdate time.Duration, retentionPeriod time.Duration) JobStore {
	if runningNoUpdate == 0 {
		runningNoUpdate = defaultMaxRunningWithNoUpdate
	}
	return &jobStore{
		cli:             cli,
		path:            path,
		consumerName:    consumerName,
		runningNoUpdate: runningNoUpdate,
		retentionPeriod: retentionPeriod,
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
	return util.RetryShort(func() error {
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

		jobResult, err := json.Marshal(result)
		if err != nil {
			return err
		}

		j.Result = string(jobResult)
		j.State = StateRunning

		return js.putJobTxn(ctx, jobKey, &j, jobKV.ModRevision)
	})
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

	j.State = StateRunning
	return js.putJobTxn(ctx, jobKey, &j, jobKV.ModRevision)

}

func (js *jobStore) Touch(ctx context.Context, jobId string) error {
	return util.RetryShort(func() error {
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

		if j.State != StateRunning{
			log.Println("touch can be done only on running state current state is : ", j.State, jobId)
			return nil
		}

		return js.putJobTxn(ctx, jobKey, &j, jobKV.ModRevision)
	})

}

func (js *jobStore) putJob(ctx context.Context, key string, job *Job, lease bool) error {
	// set a lease here for the stuff we're not going to update anymore
	job.UpdatedAt = time.Now().UTC()
	job.ConsumerName = js.consumerName

	val, err := json.Marshal(job)
	if err != nil {
		return err
	}

	if !lease {
		return js.cli.Put(ctx, key, string(val))
	}

	if js.retentionPeriod == 0 {
		return js.cli.Put(ctx, key, string(val), kvstore.WithTtl(kvstore.DefaultRetentionPeriod))
	}
	return js.cli.Put(ctx, key, string(val), kvstore.WithTtl(int64(js.retentionPeriod/time.Second)))
}

func (js *jobStore) putJobTxn(ctx context.Context, key string, job *Job, modVersion int64) error {
	// set a lease here for the stuff we're not going to update anymore
	job.UpdatedAt = time.Now().UTC()
	job.ConsumerName = js.consumerName

	val, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return js.cli.Put(
		ctx,
		key,
		string(val),
		kvstore.WithVersion(modVersion))
}

func (js *jobStore) fullPath(jobId string) string {
	return strings.Join([]string{js.path, jobId, "data"}, "/")
}

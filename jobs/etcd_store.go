package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BeameryHQ/async-stream/metrics"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"strings"
	"time"
)

type etcdJobStore struct {
	cli             *clientv3.Client
	path            string
	consumerName    string
	runningNoUpdate time.Duration
}

func NewEtcdJobStore(cli *clientv3.Client, path string, consumerName string, runningNoUpdate time.Duration) JobStore {
	return &etcdJobStore{
		cli:             cli,
		path:            path,
		consumerName:    consumerName,
		runningNoUpdate: runningNoUpdate,
	}
}

func (js *etcdJobStore) Get(ctx context.Context, jobId string) (*Job, error) {
	jobKey := js.fullPath(jobId)
	resp, err := js.cli.Get(
		ctx,
		jobKey)

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("not found %s", jobKey)
	}

	value := resp.Kvs[0].Value

	var j Job
	err = json.Unmarshal(value, &j)
	if err != nil {
		return nil, fmt.Errorf("unmarshall job %s : %v", jobId, err)
	}

	return &j, nil
}

func (js *etcdJobStore) MarkFinished(ctx context.Context, jobId string) error {
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

func (js *etcdJobStore) MarkFailed(ctx context.Context, jobId string, jobError error) error {
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

func (js *etcdJobStore) SaveResult(ctx context.Context, jobId string, result interface{}) error {
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

func (js *etcdJobStore) MarkRunning(ctx context.Context, jobId string) error {
	jobKey := js.fullPath(jobId)
	jobKV, err := js.getJobKV(ctx, jobKey)
	if err != nil {
		return nil
	}

	value := jobKV.Value

	var j Job
	err = json.Unmarshal(value, &j)
	if err != nil {
		return fmt.Errorf("unmarshall job %s : %v", jobId, err)
	}

	// check if it's already running and if can be acquired
	now := time.Now().UTC()
	lockExpiryTime := j.UpdatedAt.Add(js.runningNoUpdate)
	if j.State == StateRunning && lockExpiryTime.After(now) {
		return ErrConcurrentJobUpdate
	}

	if j.State == StateFinished {
		return fmt.Errorf("already finished nothing to do : %v", jobId)
	}

	return js.putJobTxn(ctx, jobKey, &j, jobKV.ModRevision)

}

func (js *etcdJobStore) getJobKV(ctx context.Context, jobPath string) (*mvccpb.KeyValue, error) {
	resp, err := js.cli.Get(
		ctx,
		jobPath)

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("not found %s", jobPath)
	}

	return resp.Kvs[0], nil
}

func (js *etcdJobStore) putJob(ctx context.Context, key string, job *Job, lease bool) error {
	// set a lease here for the stuff we're not going to update anymore
	job.UpdatedAt = time.Now().UTC()

	val, err := json.Marshal(job)
	if err != nil {
		return err
	}

	if !lease {
		_, err = js.cli.Put(ctx, key, string(val))
		return err
	}

	leaseResp, err := js.cli.Grant(ctx, finishedJobRetentionPeriodSec)
	if err != nil {
		return err
	}

	_, err = js.cli.Put(ctx, key, string(val), clientv3.WithLease(leaseResp.ID))
	return err
}

func (js *etcdJobStore) putJobTxn(ctx context.Context, key string, job *Job, modVersion int64) error {
	val, err := json.Marshal(job)
	if err != nil {
		return err
	}

	tx := js.cli.Txn(ctx)
	tx = tx.If(
		clientv3.Compare(clientv3.ModRevision(key), "=", modVersion),
	)

	putResp, err := tx.Then(
		clientv3.OpPut(key, string(val)),
	).Commit()

	if err != nil {
		return err
	}

	// means the if update failed maybe someone else took it
	if !putResp.Succeeded {
		return ErrConcurrentJobUpdate
	}

	return nil
}

func (js *etcdJobStore) fullPath(jobId string) string {
	return strings.Join([]string{js.path, jobId, "data"}, "/")
}

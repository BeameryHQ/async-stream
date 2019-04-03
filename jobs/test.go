package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BeameryHQ/async-stream/stream"
	"go.etcd.io/etcd/clientv3"
	"time"
)

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

package jobs

import (
	"context"
	"encoding/json"
	"go.etcd.io/etcd/clientv3"
	"strings"
)

func NewEtcdJobProducer(cli *clientv3.Client, path string) Producer {
	return &EtcdProducer{
		cli:  cli,
		path: path,
	}
}

func (p *EtcdProducer) fullPath(jobId string) string {
	return strings.Join([]string{p.path, jobId, "data"}, "/")
}

func (p *EtcdProducer) Submit(ctx context.Context, taskName string, args JobParameters) (string, error) {
	jobId, j := NewJob(taskName, args)
	jobPayload, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	jobPath := p.fullPath(jobId)
	_, err = p.cli.Put(ctx, jobPath, string(jobPayload))
	if err != nil {
		return "", err
	}

	return jobId, nil
}

func (p *EtcdProducer) SubmitWithRetry(ctx context.Context, taskName string, args JobParameters, maxRetry int) (string, error) {
	jobId, j := NewJobWithRetry(taskName, args, maxRetry)
	jobPayload, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	jobPath := p.fullPath(jobId)
	_, err = p.cli.Put(ctx, jobPath, string(jobPayload))
	if err != nil {
		return "", err
	}

	return jobId, nil
}

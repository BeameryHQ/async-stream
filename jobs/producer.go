package jobs

import (
	"context"
	"encoding/json"
	"github.com/BeameryHQ/async-stream/kvstore"
	"strings"
)

type Producer interface {
	Submit(ctx context.Context, taskName string, args JobParameters) (string, error)
	SubmitRaw(ctx context.Context, job *Job) error
	SubmitWithRetry(ctx context.Context, taskName string, args JobParameters, maxRetry int) (string, error)
}

type producerProvider struct {
	cli  kvstore.Store
	path string
}

func NewProducer(cli kvstore.Store, path string) Producer {
	return &producerProvider{
		cli:  cli,
		path: path,
	}
}

func (p *producerProvider) fullPath(jobId string) string {
	return strings.Join([]string{p.path, jobId, "data"}, "/")
}

func (p *producerProvider) Submit(ctx context.Context, taskName string, args JobParameters) (string, error) {
	jobId, j := NewJob(taskName, args)
	jobPayload, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	jobPath := p.fullPath(jobId)
	err = p.cli.Put(ctx, jobPath, string(jobPayload))
	if err != nil {
		return "", err
	}

	return jobId, nil
}

func (p *producerProvider) SubmitWithRetry(ctx context.Context, taskName string, args JobParameters, maxRetry int) (string, error) {
	jobId, j := NewJobWithRetry(taskName, args, maxRetry)
	jobPayload, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	jobPath := p.fullPath(jobId)
	err = p.cli.Put(ctx, jobPath, string(jobPayload))
	if err != nil {
		return "", err
	}

	return jobId, nil
}

func (p *producerProvider) SubmitRaw(ctx context.Context, j *Job) error {
	jobPayload, err := json.Marshal(j)
	if err != nil {
		return err
	}

	jobPath := p.fullPath(j.Id)
	return p.cli.Put(ctx, jobPath, string(jobPayload))
}

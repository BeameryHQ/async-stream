package jobs

import (
	"context"
	"encoding/json"
	"github.com/BeameryHQ/async-stream/kvstore"
	"strings"
)

type Producer interface {
	Submit(ctx context.Context, taskName string, args JobParameters, opts ...SubmitOption) (string, error)
}

type submitOption struct {
	retryCount int
}

func newSubmitOption() *submitOption {
	return &submitOption{
		retryCount: defaultMaxJobRetries,
	}
}

type producerProvider struct {
	cli  kvstore.Store
	path string
}

type SubmitOption func(option *submitOption)

func WithRetryCount(retry int) SubmitOption {
	return func(option *submitOption) {
		if retry > 0 {
			option.retryCount = retry
		}
	}
}

func NewProducer(path string, cli kvstore.Store) Producer {
	return &producerProvider{
		cli:  cli,
		path: path,
	}
}

func (p *producerProvider) fullPath(jobId string) string {
	return strings.Join([]string{p.path, jobId, "data"}, "/")
}

func (p *producerProvider) Submit(ctx context.Context, taskName string, args JobParameters, opts ...SubmitOption) (string, error) {
	option := newSubmitOption()
	for _, o := range opts {
		o(option)
	}

	jobId, j := NewJobWithRetry(taskName, args, option.retryCount)
	jobPayload, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	jobPath := p.fullPath(jobId)
	err = p.cli.Put(ctx, jobPath, string(jobPayload), kvstore.WithNoLease())
	if err != nil {
		return "", err
	}

	return jobId, nil
}

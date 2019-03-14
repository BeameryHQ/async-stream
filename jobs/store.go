package jobs

import (
	"context"
	"errors"
)

var (
	ErrConcurrentJobUpdate = errors.New("concurrent update")
)

const (
	StateFinished = "FINISHED"
	StateErrored  = "ERRORED"
	StateRunning  = "RUNNING"

	finishedJobRetentionPeriodSec = 60 * 5
)

type JobStore interface {
	Get(ctx context.Context, jobId string) (*Job, error)
	MarkFinished(ctx context.Context, jobId string) error
	MarkFailed(ctx context.Context, jobId string, err error) error
	SaveResult(ctx context.Context, jobId string, result interface{}) error
	MarkRunning(ctx context.Context, jobId string) error
}

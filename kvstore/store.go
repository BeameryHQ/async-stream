package kvstore

import (
	"context"
	"errors"
	"github.com/BeameryHQ/async-stream/stream"
)

var (
	ErrNotFound         = errors.New("record  not found")
	ErrConcurrentUpdate = errors.New("concurrent update")
)

type putConfig struct {
	disableLease bool
	version      int64
}

type PutOption func(c *putConfig)

func newPutConfig() *putConfig {
	return &putConfig{
		disableLease: false,
		version:      0,
	}
}

func WithVersion(version int64) PutOption {
	return func(c *putConfig) {
		c.version = version
	}
}

func WithNoLease() PutOption {
	return func(c *putConfig) {
		c.disableLease = true
	}
}

type Store interface {
	Get(ctx context.Context, key string) (*stream.FlowKeyValue, error)
	Put(ctx context.Context, key, value string, opts ...PutOption) error
}

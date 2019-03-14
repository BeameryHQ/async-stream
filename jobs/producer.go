package jobs

import (
	"context"
	"go.etcd.io/etcd/clientv3"
)

type Producer interface {
	Submit(ctx context.Context, taskName string, args JobParameters) (string, error)
	SubmitWithRetry(ctx context.Context, taskName string, args JobParameters, maxRetry int) (string, error)
}

type EtcdProducer struct {
	cli  *clientv3.Client
	path string
}

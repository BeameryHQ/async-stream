package horizontal

import (
	"context"
	"github.com/BeameryHQ/async-stream/lb"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"k8s.io/client-go/util/workqueue"
	"strings"
)

func NewEtcdFlowProcessor(ctx context.Context, cli *clientv3.Client, path string, consumerName string, fromEnd bool, logger *logrus.Entry, opts ...lb.Option) (Flow, error) {
	path = strings.TrimRight(path, "/")
	flow := stream.NewEtcdFlow(cli)
	etcdLb, err := lb.NewEtcdLoadBalancer(ctx, cli, path, consumerName, opts...)
	if err != nil {
		return nil, err
	}

	queue := workqueue.NewNamed("horizontal")
	handlers := []stream.FlowEventHandler{}
	cache := newFlowCache()

	logger = logger.WithField("consumerName", consumerName)

	return &FlowProcessorProvider{
		fromEnd:      fromEnd,
		flow:         flow,
		lb:           etcdLb,
		path:         path,
		handlers:     handlers,
		consumerName: consumerName,
		cache:        cache,
		logger:       logger,
		queue:        queue,
	}, nil
}

package horizontal

import (
	"context"
	"github.com/BeameryHQ/async-stream/lb"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/Sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"k8s.io/client-go/util/workqueue"
	"strings"
)



func NewEtcdHorizontalFlowProcessor(ctx context.Context, cli *clientv3.Client, path string, consumerName string, logger *logrus.Entry) (HorizontalFlow, error) {
	path = strings.TrimRight(path, "/")
	flow := stream.NewEtcdFlow(cli)
	etcdLb, err := lb.NewEtcdLoadBalancer(ctx, cli, path, consumerName)
	if err != nil {
		return nil, err
	}

	queue := workqueue.NewNamed("horizontal")
	handlers := []stream.FlowEventHandler{}
	cache := newFlowCache()

	return &FlowProcessorProvider{
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

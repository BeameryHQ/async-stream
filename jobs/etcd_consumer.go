package jobs

import (
	"context"
	"github.com/BeameryHQ/async-stream/logging"
	"github.com/BeameryHQ/async-stream/stream/horizontal"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

func NewEtcdStreamConsumer(ctx context.Context, cli *clientv3.Client, config *StreamConsumerConfiguration) (*streamConsumer, error) {
	applyDefaults(config)

	jobStore := NewEtcdJobStore(cli, config.Path, config.ConsumerName, config.RunningNoUpdate, config.RetentionPeriod)
	logger := logging.GetLogger().WithFields(logrus.Fields{
		"path": config.Path,
	})

	horizontalFlow, err := horizontal.NewEtcdFlowProcessor(ctx, cli, config.Path, config.ConsumerName, config.FromEnd, logger)
	if err != nil {
		return nil, err
	}

	return NewStreamConsumer(ctx, config, jobStore, horizontalFlow), nil
}

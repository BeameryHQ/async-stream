package jobs

import (
	"github.com/BeameryHQ/async-stream/kvstore"
	"go.etcd.io/etcd/clientv3"
)

func NewEtcdJobProducer(cli *clientv3.Client, path string) Producer {
	kv := kvstore.NewEtcdKVStore(cli)
	return &producerProvider{
		cli:  kv,
		path: path,
	}
}

package kvstore

import (
	"context"
	"github.com/BeameryHQ/async-stream/stream"
	"go.etcd.io/etcd/clientv3"
)

// 12 hrs would be the default one
const DefaultRetentionPeriod = 60 * 60 * 12

type etcdStore struct {
	cli *clientv3.Client
}

func NewEtcdKVStore(cli *clientv3.Client) Store {
	return &etcdStore{
		cli: cli,
	}
}

func (s *etcdStore) Get(ctx context.Context, key string) (*stream.FlowKeyValue, error) {
	resp, err := s.cli.Get(
		ctx,
		key)

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}

	kv := resp.Kvs[0]
	value := kv.Value
	return &stream.FlowKeyValue{
		Key:            key,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Value:          string(value),
	}, nil
}

func (s *etcdStore) Put(ctx context.Context, key, value string, opts ...PutOption) error {
	pc := newPutConfig()
	for _, o := range opts {
		o(pc)
	}

	if pc.version == 0 {
		return s.put(ctx, key, value, pc)
	}

	return s.putTxn(ctx, key, value, pc)
}

func (s *etcdStore) Delete(ctx context.Context, key string) error {
	_, err := s.cli.Delete(context.Background(), key)
	return err
}

func (s *etcdStore) put(ctx context.Context, key, value string, pc *putConfig) error {
	opts := []clientv3.OpOption{}

	if pc.ttl != 0 {
		leaseResp, err := s.cli.Grant(ctx, pc.ttl)
		if err != nil {
			return err
		}

		opts = append(opts, clientv3.WithLease(leaseResp.ID))
	}

	_, err := s.cli.Put(ctx, key, value, opts...)
	return err
}

func (s *etcdStore) putTxn(ctx context.Context, key, value string, pc *putConfig) error {
	opts := []clientv3.OpOption{}

	if pc.ttl != 0 {
		leaseResp, err := s.cli.Grant(ctx, pc.ttl)
		if err != nil {
			return err
		}

		opts = append(opts, clientv3.WithLease(leaseResp.ID))
	}

	tx := s.cli.Txn(ctx)
	tx = tx.If(
		clientv3.Compare(clientv3.ModRevision(key), "=", pc.version),
	)

	putResp, err := tx.Then(
		clientv3.OpPut(key, value, opts...),
	).Commit()

	if err != nil {
		return err
	}

	// means the if update failed maybe someone else took it
	if !putResp.Succeeded {
		return ErrConcurrentUpdate
	}

	return nil
}

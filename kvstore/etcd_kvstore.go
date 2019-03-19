package kvstore

import (
	"context"
	"github.com/BeameryHQ/async-stream/stream"
	"go.etcd.io/etcd/clientv3"
)

const defaultRetenionPeriod = 60 * 5

type etcdStore struct {
	cli *clientv3.Client
}

func NewEtcdStore(cli *clientv3.Client) Store {
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

	lease := !pc.disableLease

	if !lease {
		// if not intersted in versioning at all just insert it
		if pc.version == 0 {
			_, err := s.cli.Put(ctx, key, value)
			return err
		} else {
			modVersion := pc.version
			return s.putTxn(ctx, key, value, modVersion)
		}
	}

	leaseResp, err := s.cli.Grant(ctx, defaultRetenionPeriod)
	if err != nil {
		return err
	}

	_, err = s.cli.Put(ctx, key, value, clientv3.WithLease(leaseResp.ID))
	return err

}

func (s *etcdStore) putTxn(ctx context.Context, key, value string, modVersion int64) error {
	tx := s.cli.Txn(ctx)
	tx = tx.If(
		clientv3.Compare(clientv3.ModRevision(key), "=", modVersion),
	)

	putResp, err := tx.Then(
		clientv3.OpPut(key, value),
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

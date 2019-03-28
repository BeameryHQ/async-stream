package kvstore

import (
	"context"
	"github.com/BeameryHQ/async-stream/stream"
	"go.etcd.io/etcd/clientv3"
)

// 12 hrs would be the default one
const defaultRetentionPeriod = 60 * 60 * 12

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
	pc := NewPutConfig()
	for _, o := range opts {
		o(pc)
	}

	lease := !pc.DisableLease

	if !lease {
		// if not interested in versioning at all just insert it
		if pc.Version == 0 {
			_, err := s.cli.Put(ctx, key, value)
			return err
		} else {
			modVersion := pc.Version
			return s.putTxn(ctx, key, value, modVersion)
		}
	}

	if pc.Ttl == 0 {
		pc.Ttl = defaultRetentionPeriod
	}

	leaseResp, err := s.cli.Grant(ctx, pc.Ttl)
	if err != nil {
		return err
	}

	_, err = s.cli.Put(ctx, key, value, clientv3.WithLease(leaseResp.ID))
	return err

}

func (s *etcdStore) Delete(ctx context.Context, key string) error {
	_, err := s.cli.Delete(context.Background(), key)
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

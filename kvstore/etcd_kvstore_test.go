package kvstore

import (
	"context"
	"github.com/BeameryHQ/async-stream/tests"
	"os"
	"testing"
	"time"
)

func TestEtcdStore_Put(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kv := NewEtcdKVStore(res.Cli)

	testCases := []struct {
		name          string
		key           string
		value         string
		opts          []PutOption
		err           error
		waitRetention time.Duration
	}{
		{
			name:  "simple put",
			key:   "simple-key",
			value: "simple value",
		},
		{
			name:  "with ttl",
			key:   "ttl-key",
			value: "ttl value",
			opts: []PutOption{
				WithTtl(2),
			},
			waitRetention: time.Second * 4,
		},
		{
			name:  "with version conflict",
			key:   "ttl-version",
			value: "ttl version",
			opts: []PutOption{
				WithVersion(312434),
			},
			err: ErrConcurrentUpdate,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			keyPath := path + "/" + tc.key

			var err error

			if tc.opts != nil {
				err = kv.Put(ctx, keyPath, tc.value, tc.opts...)
			} else {
				err = kv.Put(ctx, keyPath, tc.value)
			}
			if tc.err == nil && err != nil {
				tt.Fatalf("put failed : %v", err)
			}

			if err == nil {
				val, err := kv.Get(ctx, keyPath)
				if err != nil {
					tt.Fatalf("get failed : %s", err)
				}

				if val.Value != tc.value {
					tt.Fatalf("the value we expected : %s got : %s", tc.value, val.Value)
				}
			} else {
				return
			}

			if tc.waitRetention != 0 {
				time.Sleep(tc.waitRetention)
			} else {
				return
			}

			_, err = kv.Get(ctx, keyPath)
			if err == nil {
				tt.Fatalf("retention time passed but the record still there")
			}

			if err != ErrNotFound {
				tt.Fatalf("expected error was not found got : %v", err)
			}
		})
	}
}

func TestEtcdStore_PutConcurrent(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	kv := NewEtcdKVStore(res.Cli)
	key := "concurrent"
	keyPath := path + "/" + key

	ctx := context.Background()

	err := kv.Put(ctx, keyPath, "one")
	if err != nil {
		t.Fatalf("inserting the first record failed : %v", err)
	}

	resp, err := kv.Get(ctx, keyPath)
	if err != nil {
		t.Fatalf("getting value failed : %v", err)
	}

	if resp.Value != "one"{
		t.Fatalf("mismatch of get value : ")
	}

	modVersion := resp.ModRevision

	err = kv.Put(ctx, keyPath, "two", WithVersion(modVersion))
	if err != nil {
		t.Fatalf("first concurrent update failed : %v", err)
	}

	resp, err = kv.Get(ctx, keyPath)
	if err != nil {
		t.Fatalf("getting value failed : %v", err)
	}

	if resp.Value != "two"{
		t.Fatalf("mismatch of get value : ")
	}


	err = kv.Put(ctx, keyPath, "two-concurrent", WithVersion(modVersion))
	if err == nil {
		t.Fatalf("second concurrent update didn't fail : %v", err)
	}

	if err != ErrConcurrentUpdate{
		t.Fatalf("expected concurrent error got : %v", err)
	}

	resp, err = kv.Get(ctx, keyPath)
	if err != nil {
		t.Fatalf("getting value failed : %v", err)
	}

	if resp.Value != "two"{
		t.Fatalf("mismatch of get value : ")
	}

}

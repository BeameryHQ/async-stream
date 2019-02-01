package tests

import (
	"context"
	"github.com/satori/go.uuid"
	"go.etcd.io/etcd/clientv3"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	testPrefix = "/test/"
)

type EtcdResource struct {
	Cli       *clientv3.Client
	resources map[string]bool
	mu        *sync.Mutex
	t         *testing.T
}

func NewEtcdResource(t *testing.T, server string) *EtcdResource {
	if server == "" {
		server = "localhost:2379"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{server},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		t.Fatalf("creating client %v", err)
	}

	return &EtcdResource{
		Cli:       cli,
		resources: map[string]bool{},
		mu:        &sync.Mutex{},
		t:         t,
	}
}

func (r *EtcdResource) RegisterPath(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resources[path] = true
}

func (r *EtcdResource) RegisterRandom() string {
	path := testPrefix + uuid.NewV4().String()
	r.RegisterPath(path)
	return path
}

func (r *EtcdResource) PutData(path string, values []string) [][]string {
	ctx := context.Background()
	expected := [][]string{}

	for i, v := range values {
		subPath := path + "/" + strconv.Itoa(i)
		_, err := r.Cli.Put(ctx, subPath, v)
		if err != nil {
			r.t.Fatalf("put failed : %v ", err)
		}
		expected = append(expected, []string{subPath, v})
	}

	return expected
}

func (r *EtcdResource) Cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for path := range r.resources {
		_, err := r.Cli.Delete(context.Background(), path, clientv3.WithPrefix())
		if err != nil {
			r.t.Errorf("deleting path failed : %v", err)
		}
	}

	r.Cli.Close()
}

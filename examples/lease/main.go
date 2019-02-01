package main

import (
	"context"
	"fmt"
	"github.com/satori/go.uuid"
	"go.etcd.io/etcd/clientv3"
	"log"
	"sync"
	"time"
)

const (
	jobPath   = "/hyperloop/jobs"
	nodesPath = "/hyperloop/nodes/%s"
)

func listenLogs(client *clientv3.Client, key string) {
	fmt.Println("starting listening on key : ", key)
	rch := client.Watch(context.Background(), key, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("watch : %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}

	fmt.Println("the logs channel is closed")
}

func main() {
	consumerID := uuid.NewV4()
	fmt.Println("the consumer id is : ", consumerID)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("initialization failed %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	consumerPath := fmt.Sprintf(nodesPath, consumerID)
	go listenLogs(cli, "/hyperloop")

	lease := clientv3.NewLease(cli)
	leaseResp, err := lease.Grant(ctx, 5)
	if err != nil {
		log.Fatalf("the leas grant failed  %v", err)
	}

	log.Println("the lease response is : ", leaseResp)
	resp, err := cli.Put(ctx, consumerPath, "hello", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		log.Fatalf("putting the key failed %v", err)
	}

	log.Printf("the resp is like : %+v", resp)

	log.Println("try to put a key with a lease different than the one ")
	resp, err = cli.Put(ctx, consumerPath, "hello", clientv3.WithLease(clientv3.LeaseID(12345)))
	if err != nil {
		log.Printf("[expected] putting the key failed with the wrong lease %v", err)
	}

	log.Printf("the wrong lease resp is like : %+v", resp)

	getResp, err := cli.Get(ctx, consumerPath)
	if err != nil {
		log.Fatalf("the get request failed : %v", err)
	}

	log.Println("the log resp is : ", getResp)

	keepAliveResp, err := lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		log.Fatalf("keep alive failure %v", err)
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case alive := <-keepAliveResp:
				log.Println("im alive here : ", alive)
				case <-stopCh:
					log.Println("exiting keepalive loop")
					leaseRevoke, err := lease.Revoke(ctx, leaseResp.ID)
					if err != nil{
						log.Fatalf("couldn't revoke the lease : %v", err)
					}

					log.Println("lease revoke response is like : ", leaseRevoke)
					return
			}
		}
	}()

	time.Sleep(time.Second * 10)

	log.Println("sending exit request to the keep alive")
	stopCh <- struct{}{}

	time.Sleep(time.Second * 10)
	getResp, err = cli.Get(ctx, consumerPath)
	if err != nil {
		log.Fatalf("the get request failed : %v", err)
	}

	if getResp.Kvs == nil || len(getResp.Kvs) == 0 {
		log.Println("the key doesn't exist anymore ")
		return
	}
	log.Println("the log resp is : ", getResp)

}

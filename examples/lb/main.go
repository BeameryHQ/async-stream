package main

import (
	"context"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/BeameryHQ/async-stream/lb"
	"go.etcd.io/etcd/clientv3"
	"log"
	"time"
)

func main() {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatalf("etcd client creation failed %v", err)
	}

	consumerName := "jobs-" + uuid.NewV4().String()
	ctx := context.Background()
	etcdLb, err := lb.NewEtcdLoadBalancer(ctx, cli, "/async/jobs", consumerName)
	if err != nil {
		log.Fatalf("lb init failed : %v ", err)
	}

	go func() {
		ch := etcdLb.Notify()
		for ; ; {
			select {
			case e := <-ch:
				fmt.Println("received a new lb event : ", e)
			}
		}
	}()

	ticker := time.NewTicker(1000 * time.Millisecond)
	for _ = range ticker.C {
		jobId := uuid.NewV4().String()
		target, err := etcdLb.Target(jobId, true)
		if err != nil {
			log.Fatalf("getting target faield : %v", err)
		}

		log.Printf("got target : %s", target)

	}

}

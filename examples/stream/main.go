package main

import (
	"context"
	"fmt"
	"github.com/BeameryHQ/async-stream/stream"
	"log"
	"time"

	"go.etcd.io/etcd/clientv3"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatalf("etcd client creation failed %v", err)
	}

	listItems := 0

	f := stream.NewEtcdFlow(cli)
	f.RegisterListHandler("/types/beamery/application", func(event *stream.FlowEvent) error {
		fmt.Println("got a key from key handler : ", event.Kv.Key)
		listItems++
		if listItems%30 == 0 {
			log.Println("total keys fetched so far : ", listItems)
		}
		return nil
	})
	f.Run(context.Background())
	log.Println("total items processed : ", listItems)
}

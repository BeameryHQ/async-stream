package main

import (
	"context"
	"fmt"
	"github.com/SeedJobs/async-stream/stream"
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

	f := stream.NewEtcdFlow(cli)
	f.RegisterListHandler("/async", func(event *stream.FlowEvent) error {
		fmt.Println("got a key from key handler : ", event.Kv.Key)
		fmt.Println("key handler value is : ", event.Kv.Value)
		return nil
	})
	f.RegisterWatchHandler("/async", func(event *stream.FlowEvent) error {
		fmt.Println("got a new event ", event)
		fmt.Printf("the key is %s ", string(event.Kv.Key))
		fmt.Println("the value is : ", string(event.Kv.Value))
		return nil
	})
	f.Run(context.Background())
}

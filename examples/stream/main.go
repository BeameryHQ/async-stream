package main

import (
	"context"
	"fmt"
	"github.com/BeameryHQ/async-stream/stream"
	"log"
	"net/http"
	"time"

	"go.etcd.io/etcd/clientv3"
)

func main() {
	debugPort := "9090"
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%s", debugPort), nil); err != nil {
			log.Fatalf("starting the debug server %v ", err)
		}
	}()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatalf("etcd client creation failed %v", err)
	}

	listItems := 0
	watchItems := 0

	f := stream.NewEtcdFlow(cli)
	f.RegisterListHandler("/types/beamery/application", func(event *stream.FlowEvent) error {
		fmt.Println("got a key from key handler : ", event.Kv.Key)
		listItems++
		if listItems%30 == 0 {
			log.Println("total list keys fetched so far : ", listItems)
		}
		return nil
	})
	f.RegisterWatchHandler("/types/beamery/application", func(event *stream.FlowEvent) error {
		fmt.Println("got a key from watch handler : ", event.Kv.Key)
		watchItems++
		if watchItems%30 == 0 {
			log.Println("total watch keys fetched so far : ", watchItems)
		}
		return nil
	})
	f.Run(context.Background())
	log.Println("total items processed : ", listItems)
}

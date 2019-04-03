package main

import (
	"context"
	"fmt"
	"github.com/BeameryHQ/async-stream/jobs"
	"go.etcd.io/etcd/clientv3"
	"log"
	"strconv"
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

	p := jobs.NewEtcdJobProducer(cli, "/async/jobs")

	taskName := "aws.bucketProcessor"
	ctx := context.Background()

	log.Print("starting up ")
	for range time.Tick(time.Second * 45) {
		for i := 0; i < 20; i++ {
			jobId, err := p.Submit(ctx, taskName, jobs.JobParameters{
				"bucket": fmt.Sprintf("bucket_%s", strconv.Itoa(i)),
			},
			)

			if err != nil {
				log.Fatalf("submitting the job failed : %v", err)
			}

			log.Println("Job Id was created : ", jobId)
		}
	}

}

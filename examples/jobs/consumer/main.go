package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/BeameryHQ/async-stream/jobs"
	"go.etcd.io/etcd/clientv3"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	consumerPrefix := os.Getenv("CONSUMER_PREFIX")
	if consumerPrefix == "" {
		log.Fatal("CONSUMER_PREFIX is required")
	}

	consumerIndex, err := strconv.Atoi(os.Getenv("CONSUMER_INDEX"))
	if err != nil {
		log.Fatal("CONSUMER_INDEX is required")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatalf("etcd client creation failed %v", err)
	}

	//jobFilter := lb.NewStaticLbFromIndexes(consumerPrefix, consumerIndex, consumerCount)
	ctx := context.Background()

	path := "/async/jobs"
	consumerName := fmt.Sprintf("%s-%d", consumerPrefix, consumerIndex)
	config := &jobs.StreamConsumerConfiguration{
		Path:         path,
		ConsumerName: consumerName,
		Concurrency:  10,
	}

	c, err := jobs.NewEtcdStreamConsumer(
		ctx,
		cli,
		config,
	)

	if err != nil {
		log.Fatalf("creating consumer : %v", err)
	}

	c.RegisterHandler("aws.bucketProcessor", &timeSleepHandler{})
	c.Start(true)

}

type Progress struct {
	Items []string `json:"Items"`
}

type timeSleepHandler struct {
}

func (t *timeSleepHandler) Handle(jobStore jobs.JobStore, j *jobs.Job, logger *logrus.Entry) error {
	logger.Println("start processing")

	time.Sleep(time.Second * 3)

	r := rand.Intn(10)
	if r == 0 {
		logger.Errorf("hit an err condition pre save failing ")
		return fmt.Errorf("hit by err pre save")
	}

	progress := &Progress{}
	if j.Result != "" {
		err := json.Unmarshal([]byte(j.Result), progress)
		if err != nil {
			return fmt.Errorf("umarshall result : %v", err)
		}
	}

	progress.Items = append(progress.Items, time.Now().UTC().String())
	err := jobStore.SaveResult(context.Background(), j.Id, progress)
	if err != nil {
		return fmt.Errorf("saving the progress failed : %v", err)
	}

	r = rand.Intn(10)
	if r == 1 {
		logger.Error("hit an error condition post save failing ")
		panic(fmt.Errorf("hit by 25 percentaile chance post save"))
	}

	if err != nil {
		logger.Error("saving progress failed ", err)
	}
	time.Sleep(time.Second * 3)
	logger.Println("finished job")
	return nil
}

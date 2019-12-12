package horizontal

import (
	"context"
	"fmt"
	"github.com/BeameryHQ/async-stream/lb"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/BeameryHQ/async-stream/tests"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestFlowProcessorProvider_Run(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.WithField("case", "test")
	f, err := NewEtcdFlowProcessor(
		ctx,
		res.Cli,
		path,
		"consumer-0",
		false,
		logger,
		lb.WithSetleTime(time.Second))

	if err != nil {
		t.Fatalf("init horizontal flow failed %v", err)
	}

	recvdValsChan := make(chan []string)

	f.RegisterHandler(func(event *stream.FlowEvent) error {
		log.Println("event val ", event.Kv.Value)
		recvdValsChan <- []string{event.Kv.Key, event.Kv.Value}
		log.Println("event val was sent ", event.Kv.Value)
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		err = f.Run(ctx, true)
		if err != nil {
			t.Fatalf("run failed : %v", err)
		}
	}()

	values := []string{"one", "two", "threee"}
	expectedVals := res.PutData(path, values)

	expectedValsMap := map[string]string{}
	for _, e := range expectedVals {
		expectedValsMap[e[0]] = e[1]
	}

L:
	for {
		select {
		case pair := <-recvdValsChan:
			key := pair[0]
			value := pair[1]

			expectedVal := expectedValsMap[key]

			if expectedVal != "" && expectedVal != value {
				t.Fatalf("expected values was : %s got : %s", expectedVal, value)
			}

			if expectedVal != "" {
				delete(expectedValsMap, key)
			}
		case <-time.After(time.Second * 10):
			fmt.Println("time out exiting")
			break L
		}
	}

	if len(expectedValsMap) != 0 {
		t.Fatalf("some values were not receieved : %+v", expectedValsMap)
	}

	cancel()
	wg.Wait()
}

func TestFlowProcessorProvider_RunMultiple(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.WithField("case", "test")
	f, err := NewEtcdFlowProcessor(
		ctx,
		res.Cli,
		path,
		"consumer-0",
		false,
		logger,
		lb.WithSetleTime(time.Second))

	if err != nil {
		t.Fatalf("init horizontal flow failed %v", err)
	}

	f1, err := NewEtcdFlowProcessor(
		ctx,
		res.Cli,
		path,
		"consumer-1",
		false,
		logger,
		lb.WithSetleTime(time.Second))

	if err != nil {
		t.Fatalf("init horizontal flow failed %v", err)
	}

	recvdValsChan := make(chan []string)

	handler := func(event *stream.FlowEvent) error {
		log.Println("event val ", event.Kv.Value)
		recvdValsChan <- []string{event.Kv.Key, event.Kv.Value}
		log.Println("event val was sent ", event.Kv.Value)
		return nil
	}

	f.RegisterHandler(handler)
	f1.RegisterHandler(handler)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		err = f.Run(ctx, true)
		if err != nil {
			t.Fatalf("run failed : %v", err)
		}

	}()

	go func() {
		defer wg.Done()

		err = f1.Run(ctx, true)
		if err != nil {
			t.Fatalf("run failed : %v", err)
		}

	}()

	values := []string{"one", "two", "three", "four", "five", "six"}
	expectedVals := res.PutData(path, values)

	expectedValsMap := map[string]string{}
	for _, e := range expectedVals {
		expectedValsMap[e[0]] = e[1]
	}

L:
	for {
		select {
		case pair := <-recvdValsChan:
			key := pair[0]
			value := pair[1]

			expectedVal := expectedValsMap[key]

			if expectedVal != "" && expectedVal != value {
				t.Fatalf("expected values was : %s got : %s", expectedVal, value)
			}

			if expectedVal != "" {
				delete(expectedValsMap, key)
			}
		case <-time.After(time.Second * 10):
			fmt.Println("time out exiting")
			break L
		}
	}

	if len(expectedValsMap) != 0 {
		t.Fatalf("some values were not receieved : %+v", expectedValsMap)
	}

	cancel()
	wg.Wait()
}

func TestFlowProcessorProvider_RunMultipleReProcess(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	logger := logrus.WithField("case", "test")
	f, err := NewEtcdFlowProcessor(
		ctx,
		res.Cli,
		path,
		"consumer-0",
		false,
		logger,
		lb.WithSetleTime(time.Second))

	if err != nil {
		t.Fatalf("init horizontal flow failed %v", err)
	}

	f1, err := NewEtcdFlowProcessor(
		ctx1,
		res.Cli,
		path,
		"consumer-1",
		false,
		logger,
		lb.WithSetleTime(time.Second))

	if err != nil {
		t.Fatalf("init horizontal flow failed %v", err)
	}

	recvdValsChan := make(chan []string)

	handler := func(event *stream.FlowEvent) error {
		log.Println("event val ", event.Kv.Value)
		recvdValsChan <- []string{event.Kv.Key, event.Kv.Value}
		log.Println("event val was sent ", event.Kv.Value)
		return nil
	}

	handler2Values := [][]string{}
	var muHandlerValues sync.Mutex

	handler2 := func(event *stream.FlowEvent) error {
		muHandlerValues.Lock()
		defer muHandlerValues.Unlock()

		log.Println("event val ", event.Kv.Value)
		p := []string{event.Kv.Key, event.Kv.Value}
		recvdValsChan <- p
		log.Println("event val was sent ", event.Kv.Value)
		handler2Values = append(handler2Values, p)
		return nil
	}

	f.RegisterHandler(handler)
	f1.RegisterHandler(handler2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		err = f.Run(ctx, true)
		if err != nil {
			t.Fatalf("run failed : %v", err)
		}

	}()

	go func() {
		defer wg.Done()

		err = f1.Run(ctx1, true)
		if err != nil {
			t.Fatalf("run failed : %v", err)
		}

	}()

	values := []string{"one", "two", "three", "four", "five", "six"}
	expectedVals := res.PutData(path, values)

	expectedValsMap := map[string]string{}
	for _, e := range expectedVals {
		expectedValsMap[e[0]] = e[1]
	}

L:
	for {
		select {
		case pair := <-recvdValsChan:
			key := pair[0]
			value := pair[1]

			expectedVal := expectedValsMap[key]

			if expectedVal != "" && expectedVal != value {
				t.Fatalf("expected values was : %s got : %s", expectedVal, value)
			}

			if expectedVal != "" {
				delete(expectedValsMap, key)
			}
		case <-time.After(time.Second * 10):
			fmt.Println("time out exiting")
			break L
		}
	}

	if len(expectedValsMap) != 0 {
		t.Fatalf("some values were not receieved : %+v", expectedValsMap)
	}

	// try to stop consumer-1 and check to see if will get the items it processed from channel
	cancel1()

	expectedValsMap1 := map[string]string{}
	for _, e := range handler2Values {
		expectedValsMap1[e[0]] = e[1]
	}

	t.Logf("expectedValsMap1 are %v", expectedValsMap1)

L2:
	for {
		select {
		case pair := <-recvdValsChan:
			key := pair[0]
			value := pair[1]

			expectedVal := expectedValsMap1[key]

			if expectedVal != "" && expectedVal != value {
				t.Fatalf("expected values was : %s got : %s", expectedVal, value)
			}

			if expectedVal != "" {
				delete(expectedValsMap1, key)
			}
		case <-time.After(time.Second * 10):
			fmt.Println("time out exiting")
			break L2
		}
	}

	if len(expectedValsMap1) != 0 {
		t.Fatalf("re-processing failed some values were not receieved : %+v", expectedValsMap)
	}

	cancel()
	wg.Wait()
}

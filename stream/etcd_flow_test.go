// +build integration

package stream

import (
	"context"
	"github.com/BeameryHQ/async-stream/tests"
	"github.com/go-test/deep"
	"os"
	"sync"
	"testing"
)

func TestEtcdFlow_RegisterListHandler(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	t.Logf("testing the etcd flow %s", path)

	ctx := context.Background()

	expectedValues := []string{
		"foo",
		"bar",
		"flex",
	}

	expected := res.PutData(path, expectedValues)

	actual := [][]string{}
	flow := NewEtcdFlow(res.Cli)
	flow.RegisterListHandler(
		path,
		func(event *FlowEvent) error {
			//t.Logf("list handler %+v", event)
			actual = append(actual, []string{
				event.Kv.Key,
				event.Kv.Value,
			})

			return nil
		})
	flow.Run(ctx)

	if diff := deep.Equal(expected, actual); diff != nil {
		t.Fatalf("list handler failure : %v", diff)
	}
}

func TestEtcdFlow_RegisterListHandler_Pagination(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	t.Logf("testing the etcd flow %s", path)

	ctx := context.Background()

	expectedValues := []string{
		"foo",
		"bar",
		"flex",
		"rex",
		"zoo",
		"moo",
	}

	expected := res.PutData(path, expectedValues)

	actual := [][]string{}
	flow := NewEtcdFlow(res.Cli, WithPageSize(2))
	flow.RegisterListHandler(
		path,
		func(event *FlowEvent) error {
			//t.Logf("list handler %+v", event)
			actual = append(actual, []string{
				event.Kv.Key,
				event.Kv.Value,
			})

			return nil
		})
	flow.Run(ctx)

	if diff := deep.Equal(expected, actual); diff != nil {
		t.Fatalf("list handler failure : %v", diff)
	}
}

func TestEtcdFlow_RegisterWatchHandler(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	t.Logf("testing the etcd flow %s", path)

	ctx := context.Background()

	expectedListValues := []string{
		"foo",
		"bar",
		"flex",
	}
	expected := res.PutData(path, expectedListValues)

	expectedWatchValues := []string{
		"fooUpdate",
		"barUpdat",
		"flexUdate",
		"newOne",
	}

	actualList := [][]string{}
	actualWatch := [][]string{}
	listingDone := make(chan bool)
	watchingDone := make(chan bool)

	flow := NewEtcdFlow(res.Cli)
	flow.RegisterListHandler(
		path,
		func(event *FlowEvent) error {
			//t.Logf("list event type : %s", event.Type)
			//t.Logf("lidt event key %s", event.Kv.Key)
			//t.Logf("list event value %s", event.Kv.Value)

			actualList = append(actualList, []string{
				event.Kv.Key,
				event.Kv.Value,
			})

			if len(actualList) == len(expected) {
				listingDone <- true
			}

			return nil
		})

	flow.RegisterWatchHandler(path,
		func(event *FlowEvent) error {
			//fmt.Printf("watch event type : %s \n", event.Type)
			//fmt.Printf("watch event key %s\n", event.Kv.Key)
			//fmt.Printf("watch event value %s\n", event.Kv.Value)

			if event.IsDeleted() {
				return nil
			}
			actualWatch = append(actualWatch, []string{
				event.Kv.Key,
				event.Kv.Value,
			})

			if len(actualWatch) == len(expectedWatchValues) {
				watchingDone <- true
			}

			return nil
		})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		flow.Run(ctx)

	}()

	t.Logf("waiting for listing to be over")
	<-listingDone

	expectedWatch := res.PutData(path, expectedWatchValues)

	t.Logf("waiting for watch handler to be over")
	<-watchingDone

	if diff := deep.Equal(expected, actualList); diff != nil {
		t.Fatalf("list handler failure : %v", diff)
	}
	if diff := deep.Equal(expectedWatch, actualWatch); diff != nil {
		t.Fatalf("watch handler failure : %v", diff)
	}

	// wait here for the flow to exit properly
}

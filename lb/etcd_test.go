// +build integration

package lb

import (
	"context"
	"github.com/BeameryHQ/async-stream/tests"
	"github.com/davecgh/go-spew/spew"
	"github.com/satori/go.uuid"
	"os"
	"sync"
	"testing"
	"time"
)

func TestEtcdBakedLoadBalancer_Target(t *testing.T) {
	t.Logf("Target testing")

	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	ctx := context.Background()
	currentTarget := "lb-0"
	lb, err := NewEtcdLoadBalancer(
		ctx,
		res.Cli,
		path,
		currentTarget,
		WithSetleTime(time.Second*1),
	)
	if err != nil {
		t.Fatalf("creating lb failed")
	}

	var keys []string
	for i := 0; i < 10; i++ {
		keys = append(keys, uuid.NewV4().String())
	}

	for _, k := range keys {
		target, err := lb.Target(k, true)
		if err != nil {
			t.Fatalf("target failed : %v", err)
		}

		if target != currentTarget {
			t.Fatalf("target mismatch expexted %s got %s", currentTarget, target)
		}
	}

	lb.Close()

}

func TestEtcdBakedLoadBalancer_Notify(t *testing.T) {
	server := os.Getenv("ETCD_SERVER")
	res := tests.NewEtcdResource(t, server)
	path := res.RegisterRandom()
	defer res.Cleanup()

	ctx := context.Background()
	firstTarget := "lb-0"
	secondTarget := "lb-1"
	lb1, err := NewEtcdLoadBalancer(
		ctx,
		res.Cli,
		path,
		firstTarget,
		WithSetleTime(time.Second*3),
	)
	if err != nil {
		t.Fatalf("creating lb1 failed")
	}

	lb2, err := NewEtcdLoadBalancer(
		ctx,
		res.Cli,
		path,
		secondTarget,
		WithSetleTime(time.Second*1),
	)
	if err != nil {
		t.Fatalf("creating lb2 failed")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		t.Logf("wait lb1")
		waitForEvents(t, lb1, []*LbEvent{
			NewAddedEvent(secondTarget),
		})

	}()

	go func() {
		wg.Done()
		t.Logf("wait lb2")
		waitForEvents(t, lb2, []*LbEvent{
			NewAddedEvent(firstTarget),
		})

	}()

	wg.Wait()

	var keys []string
	for i := 0; i < 10; i++ {
		keys = append(keys, uuid.NewV4().String())
	}

	for _, k := range keys {
		target, err := lb1.Target(k, true)
		if err != nil {
			t.Fatalf("target failed lb1 : %v", err)
		}

		if target != firstTarget && target != secondTarget {
			t.Fatalf("target mismatch expected %s, %s got %s", firstTarget, secondTarget, target)
		}

		target, err = lb2.Target(k, true)
		if err != nil {
			t.Fatalf("target failed lb2 : %v", err)
		}

		if target != firstTarget && target != secondTarget {
			t.Fatalf("target mismatch expected %s, %s got %s", firstTarget, secondTarget, target)
		}
	}

	// if close the second one will should get a removed on lb1
	lb2.Close()

	t.Logf("wait for the lb2 removed")
	waitForEvents(t, lb1, []*LbEvent{
		NewRemovedEvent(secondTarget),
	})

	// also stop the first one and also listen for the
	lb1.Close()
	t.Logf("wait for the lb1.close")
	waitForEvents(t, lb1, []*LbEvent{
		NewLbStoppedEvent(),
	})

}

func waitForEvents(t *testing.T, l LbNotifier, events []*LbEvent) {
	notifyEventsChan := l.NotifyBulk()
	var notifyEvents []*LbEvent

	select {
	case nevents, ok := <-notifyEventsChan:
		notifyEvents = nevents
		if !ok {
			for _, e := range events {
				if e.Event == LbStopped {
					return
				}
			}
			t.Fatalf("the channel was closed")
		}

	case <-time.After(time.Second * 10):
		t.Fatalf("timeout waiting for the events")
		return
	}

	if len(events) != len(notifyEvents) {
		t.Errorf("bulk event failed expected : %d events got %d", len(events), len(notifyEvents))
		t.Fatalf("expected : %+v got : %+v", spew.Sdump(events), spew.Sdump(notifyEvents))
	}

	for i, e := range events {
		notifyEvent := notifyEvents[i]
		if e.Event != notifyEvent.Event || e.Target != notifyEvent.Target {
			t.Fatalf("event mismatch expected : %+v, got : %+v", e, notifyEvent)
		}
	}
}

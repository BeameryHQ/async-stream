// +build integration

package lb

import (
	"context"
	"github.com/satori/go.uuid"
	"github.com/SeedJobs/async-stream/tests"
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
		WithSetleTime(time.Second * 1),
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
	t.Logf("notify test ")

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
		t.Fatalf("creating lb failed")
	}

	var keys []string
	for i := 0; i < 10; i++ {
		keys = append(keys, uuid.NewV4().String())
	}

	for _, k := range keys {
		target, err := lb1.Target(k, true)
		if err != nil {
			t.Fatalf("target failed : %v", err)
		}

		if target != firstTarget {
			t.Fatalf("target mismatch expexted %s got %s", firstTarget, target)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		wg.Done()
		e := <- lb1.Notify()
		if e.Event != TargetAdded{
			t.Errorf("event type expected : %s got : %s", TargetAdded, e.Event)
		}

		if e.Target != secondTarget{
			t.Errorf("event target expected : %s got : %s", secondTarget, e.Target)
		}
	}()

	lb2, err := NewEtcdLoadBalancer(
		ctx,
		res.Cli,
		path,
		secondTarget,
		WithSetleTime(time.Second*3),
	)
	if err != nil {
		t.Fatalf("creating lb failed")
	}

	wg.Wait()

	for _, k := range keys {
		target, err := lb2.Target(k, true)
		if err != nil {
			t.Fatalf("target failed : %v", err)
		}

		if target != firstTarget && target != secondTarget{
			t.Fatalf("target mismatch expexted %s, %s got %s", firstTarget, secondTarget, target)
		}
	}

	lb2.Close()

	var wg2 sync.WaitGroup
	wg2.Add(1)

	go func() {
		wg2.Done()
		e := <- lb1.Notify()
		if e.Event != TargetRemoved{
			t.Errorf("event type expected : %s got : %s", TargetRemoved, e.Event)
		}

		if e.Target != secondTarget{
			t.Errorf("event target expected : %s got : %s", secondTarget, e.Target)
		}
	}()

	wg2.Wait()
	lb1.Close()
}

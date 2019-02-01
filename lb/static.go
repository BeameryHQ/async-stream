package lb

import (
	"fmt"
	"github.com/serialx/hashring"
)

type staticLb struct {
	mykey   string
	allKeys []string
	ring    *hashring.HashRing
}

func NewStaticLB(mykey string, allKeys []string) KeyLoadBalancer {

	ring := hashring.New(allKeys)

	return &staticLb{
		mykey:   mykey,
		allKeys: allKeys,
		ring:    ring,
	}
}

func (h *staticLb) Target(key string, waitSettleTime bool) (string, error) {

	server, ok := h.ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("not found : %s", key)
	}

	return server, nil
}


func (h *staticLb) Close() {

}

func NewStaticLbFromIndexes(prefix string, myIndex, all int) KeyLoadBalancer {
	if myIndex > all-1 {
		panic("myindex out of range")
	}

	myKey := fmt.Sprintf("%s-%d", prefix, myIndex)
	allKeys := []string{}

	for i := 0; i < all; i++ {
		allKeys = append(allKeys, fmt.Sprintf("%s-%d", prefix, i))
	}

	fmt.Println("all keys ", allKeys)
	return NewStaticLB(myKey, allKeys)
}

package lb

import "time"

type KeyLoadBalancer interface {
	Target(key string, waitSettleTime bool) (string, error)
	Close()
}

const (
	TargetAdded   = "Added"
	TargetRemoved = "Removed"
)

type LbEvent struct {
	CreatedOn time.Time
	Event     string
	Target    string
}

func NewAddedEvent(target string) *LbEvent {
	return &LbEvent{
		CreatedOn: time.Now().UTC(),
		Event:     TargetAdded,
		Target:    target,
	}
}

func NewRemovedEvent(target string) *LbEvent {
	return &LbEvent{
		CreatedOn: time.Now().UTC(),
		Event:     TargetRemoved,
		Target:    target,
	}
}

type LbNotifier interface {
	Notify() <-chan *LbEvent
}

type KeyLbNotifier interface {
	KeyLoadBalancer
	LbNotifier
}

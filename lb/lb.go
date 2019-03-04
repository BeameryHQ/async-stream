package lb

import "time"

type KeyLoadBalancer interface {
	Target(key string, waitSettleTime bool) (string, error)
	Close()
}

const (
	TargetAdded   = "Added"
	TargetRemoved = "Removed"
	LbStopped     = "LbStopped"
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

func NewLbStoppedEvent() *LbEvent {
	return &LbEvent{
		CreatedOn: time.Now().UTC(),
		Event:     LbStopped,
	}
}

type LbNotifier interface {
	Notify() <-chan *LbEvent
	NotifyBulk() <-chan []*LbEvent
}

type KeyLbNotifier interface {
	KeyLoadBalancer
	LbNotifier
}

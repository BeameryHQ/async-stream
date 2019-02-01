package lb

type KeyLoadBalancer interface {
	Target(key string, waitSettleTime bool) (string, error)
	Close()
}

const (
	TargetAdded   = "Added"
	TargetRemoved = "Removed"
)

type LbEvent struct {
	Event  string
	Target string
}

type LbNotifier interface {
	Notify() <-chan *LbEvent
}

type KeyLbNotifier interface {
	KeyLoadBalancer
	LbNotifier
}

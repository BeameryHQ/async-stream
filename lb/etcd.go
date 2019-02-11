package lb

import (
	"context"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/serialx/hashring"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/BeameryHQ/async-stream/logging"
	"go.etcd.io/etcd/clientv3"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	defaultLbSubPath  = "/lb"
	defaultSettleTime = time.Second * 10
)

type etcdBakedLoadBalancer struct {
	ctx            context.Context
	cancelFunc     context.CancelFunc
	logger         *logrus.Entry
	cli            *clientv3.Client
	path           string
	ring           *hashring.HashRing
	myTarget       string
	ringLock       *sync.Mutex
	cache          map[string]bool
	lastRingUpdate time.Time
	notifyChan     chan *LbEvent
	settleTime     time.Duration
}

type optionalConfig struct {
	lbSubpath  string
	settleTime time.Duration
}

type Option func(config *optionalConfig)

func WithSetleTime(d time.Duration) Option {
	return func(config *optionalConfig) {
		config.settleTime = d
	}
}

func WithSubPath(subPath string) Option {
	return func(config *optionalConfig) {
		config.lbSubpath = subPath
	}
}

func applyOpts(config *optionalConfig) {
	if config.settleTime == 0 {
		config.settleTime = defaultSettleTime
	}

	if config.lbSubpath == "" {
		config.lbSubpath = defaultLbSubPath
	}
}

func NewEtcdLoadBalancer(ctx context.Context, cli *clientv3.Client, path string, myTarget string, opts ...Option) (KeyLbNotifier, error) {
	logger := logging.GetLogger().WithFields(logrus.Fields{
		"component":     "lb",
		"path":          path,
		"currentTarget": myTarget,
	})

	config := &optionalConfig{}
	for _, o := range opts {
		o(config)
	}
	applyOpts(config)

	logger.Debug("starting etcd lb with config : ", config)

	ring := hashring.New([]string{myTarget})
	path = strings.TrimRight(path, "/") + config.lbSubpath

	notifyChan := make(chan *LbEvent)

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	l := &etcdBakedLoadBalancer{
		ctx:        ctx,
		cancelFunc: cancel,
		logger:     logger,
		cli:        cli,
		path:       path,
		ring:       ring,
		myTarget:   myTarget,
		ringLock:   &sync.Mutex{},
		cache: map[string]bool{
			myTarget: true,
		},
		notifyChan: notifyChan,
		settleTime: config.settleTime,
	}

	if err := l.addTargetRemote(ctx, myTarget); err != nil {
		return nil, err
	}

	go func() {
		l.logger.Debugln("starting the lb stream flow on ", l.path)
		f := stream.NewEtcdFlow(l.cli)
		f.RegisterListHandler(l.path, l.keyHandler)
		f.RegisterWatchHandler(l.path, l.watchHandler)
		f.Run(l.ctx)
	}()

	return l, nil
}

func (l *etcdBakedLoadBalancer) Target(key string, waitSettleTime bool) (string, error) {
	if waitSettleTime {
		for {
			now := time.Now().UTC()
			lastUpdate := l.getLastUpdated()
			if lastUpdate.Add(l.settleTime).After(now) {
				runIn := lastUpdate.Add(l.settleTime).Sub(now)
				l.logger.Debugln("waiting for settle time : ", runIn)
				time.Sleep(runIn)
			} else {
				break
			}
		}
	}

	return l.getNodeSafe(key)
}

func (l *etcdBakedLoadBalancer) Notify() <-chan *LbEvent {
	return l.notifyChan
}

func (l *etcdBakedLoadBalancer) Close() {
	l.cancelFunc()
}

// should only be called inside of concurrently safe methods
func (l *etcdBakedLoadBalancer) sendNotification(e *LbEvent) error {
	now := time.Now().UTC()

	sendToChan := func(e *LbEvent) {
		select {
		case l.notifyChan <- e:
			l.logger.Debugln("send notification : ", e)
		default:
			return
		}
	}

	lastUpdate := l.lastRingUpdate
	if lastUpdate.Add(l.settleTime).After(now) {
		runIn := lastUpdate.Add(l.settleTime).Sub(now)
		go func() {
			l.logger.Debugln("going to send notification in : ", runIn)
			<-time.After(runIn)
			sendToChan(e)
		}()
	} else {
		sendToChan(e)
	}

	return nil
}

func (l *etcdBakedLoadBalancer) getNodeSafe(key string) (string, error) {
	l.ringLock.Lock()
	defer l.ringLock.Unlock()

	target, ok := l.ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("no target found %s", target)
	}

	return target, nil
}

func (l *etcdBakedLoadBalancer) getLastUpdated() time.Time {
	l.ringLock.Lock()
	defer l.ringLock.Unlock()

	return l.lastRingUpdate
}

func (l *etcdBakedLoadBalancer) registerTarget(target string, skipNotification bool) error {
	l.ringLock.Lock()
	defer l.ringLock.Unlock()

	if l.cache[target] {
		return nil
	}

	l.cache[target] = true
	l.ring = l.ring.AddNode(target)
	l.lastRingUpdate = time.Now().UTC()

	if !skipNotification {
		_ = l.sendNotification(&LbEvent{
			Event:  TargetAdded,
			Target: target,
		})
	}

	l.logger.Debugln("registered a new target : ", target, l.cache)

	return nil
}

func (l *etcdBakedLoadBalancer) removeTarget(target string) error {
	l.ringLock.Lock()
	defer l.ringLock.Unlock()

	delete(l.cache, target)
	l.ring = l.ring.RemoveNode(target)
	l.lastRingUpdate = time.Now().UTC()

	_ = l.sendNotification(&LbEvent{
		Event:  TargetRemoved,
		Target: target,
	})

	l.logger.Debugln("removed a target : ", target, l.cache)
	return nil
}

func (l *etcdBakedLoadBalancer) addTargetRemote(ctx context.Context, target string) error {
	key := l.path + "/" + target

	lease := clientv3.NewLease(l.cli)
	leaseResp, err := lease.Grant(ctx, 5)
	if err != nil {
		return err
	}

	_, err = l.cli.Put(ctx, key, "", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return err
	}

	keepAliveResp, err := lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return fmt.Errorf("keepalive failed : %v", err)
	}

	go func() {
		// TODO: do something about when we get an uncoverable resp
		for ; ; {
			select {
			case _ = <-keepAliveResp:
			//l.logger.Println("keepalive beat ...", alive)
			case <-l.ctx.Done():
				l.logger.Debug("stop signal received exiting keep alive loop")
				return
			}
		}
	}()

	l.lastRingUpdate = time.Now().UTC()
	return nil
}

func (l *etcdBakedLoadBalancer) extractTargetFromKey(key string) (string, error) {
	pathPrefix := strings.TrimRight(l.path, "/") + "/"
	regexStr := pathPrefix + `(.+)`
	r, err := regexp.Compile(regexStr)
	if err != nil {
		return "", err
	}

	parts := r.FindStringSubmatch(strings.TrimRight(string(key), "/"))
	if len(parts) != 2 {
		return "", fmt.Errorf("target not found in the path : %s", string(key))
	}

	return parts[1], nil
}

func (l *etcdBakedLoadBalancer) keyHandler(event *stream.FlowEvent) error {

	target, err := l.extractTargetFromKey(string(event.Kv.Key))
	if err != nil {
		return err
	}

	return l.registerTarget(target, true)
}

func (l *etcdBakedLoadBalancer) watchHandler(event *stream.FlowEvent) error {
	target, err := l.extractTargetFromKey(string(event.Kv.Key))
	if err != nil {
		return err
	}

	if event.IsCreated() || event.IsUpdated() {
		return l.registerTarget(target, false)
	}

	return l.removeTarget(target)
}

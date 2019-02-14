package lb

import (
	"context"
	"errors"
	"fmt"
	"github.com/BeameryHQ/async-stream/logging"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/Sirupsen/logrus"
	"github.com/serialx/hashring"
	"go.etcd.io/etcd/clientv3"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	DownErr   = errors.New("lb down")
	PausedErr = errors.New("lb paused")
)

const (
	defaultLbSubPath  = "/lb"
	defaultSettleTime = time.Second * 20

	pauseCmd   = "PAUSE"
	unpauseCmd = "UNPAUSE"
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
	stopped        bool
	stoppedMu      *sync.Mutex
	lbPauseChan    chan string
	lbSettleChan   chan time.Time
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
		notifyChan:   notifyChan,
		settleTime:   config.settleTime,
		stoppedMu:    &sync.Mutex{},
		lbPauseChan:  make(chan string, 100),
		lbSettleChan: make(chan time.Time, 100),
	}

	go l.monitorLbPause()

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
			stopped, err := l.isStopped()
			if err != nil {
				return "", err
			}

			if !stopped {
				break
			}
			logrus.Debug("the lb is locked we sleep")
			time.Sleep(time.Second * 5)
		}
	}

	stopped, err := l.isStopped()
	if err != nil {
		return "", err
	}

	if stopped {
		return "", PausedErr
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

	l.setPauseSettle()
	if target == l.myTarget {
		l.clearPause()
		skipNotification = true
	}

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

	l.setPauseSettle()
	skipNotification := false
	if target == l.myTarget {
		l.clearPause()
		skipNotification = true
	}

	delete(l.cache, target)
	l.ring = l.ring.RemoveNode(target)
	l.lastRingUpdate = time.Now().UTC()

	if !skipNotification {
		_ = l.sendNotification(&LbEvent{
			Event:  TargetRemoved,
			Target: target,
		})
	}

	l.logger.Debugln("removed a target : ", target, l.cache)
	return nil
}

func (l *etcdBakedLoadBalancer) addTargetRemote(ctx context.Context, target string) error {
	key := l.path + "/" + target

	keepAliveChan, err := l.getKeepAliveChan(ctx, key)
	if err != nil {
		return err
	}

	go func() {

		for ; ; {
			select {
			case _, ok := <-keepAliveChan:
				if !ok {
					l.setPause()
					l.logger.Errorf("keepalive channel was closed, will retry to recover")
					keepAliveChan = nil
					err = RetryNormal(func() error {
						l.logger.Warningf("retry to recover the channel")
						var err error
						keepAliveChan, err = l.getKeepAliveChan(ctx, key)
						return err
					})

					if err != nil {
						l.logger.Errorf("keepalive channel was closed and can't be recovered exiting")
						// make sure we stop the whole thing
						l.cancelFunc()
						return
					}
					l.clearPause()
				}
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

func (l *etcdBakedLoadBalancer) getKeepAliveChan(ctx context.Context, key string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	lease := clientv3.NewLease(l.cli)
	leaseResp, err := lease.Grant(ctx, 5)
	if err != nil {
		return nil, err
	}

	_, err = l.cli.Put(ctx, key, "", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return nil, err
	}

	return lease.KeepAlive(ctx, leaseResp.ID)
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

func (l *etcdBakedLoadBalancer) isStopped() (bool, error) {
	l.stoppedMu.Lock()
	defer l.stoppedMu.Unlock()

	if l.ctx.Err() != nil {
		l.logger.Errorf("lb is down, can't continue %v", l.ctx.Err())
		return false, DownErr
	}

	return l.stopped, nil
}

func (l *etcdBakedLoadBalancer) setStop(val bool) {
	l.stoppedMu.Lock()
	defer l.stoppedMu.Unlock()
	l.stopped = val
}

func (l *etcdBakedLoadBalancer) setPause() {
	l.setStop(true)
	l.lbPauseChan <- pauseCmd
}

func (l *etcdBakedLoadBalancer) clearPause() {
	l.lbPauseChan <- unpauseCmd
}

func (l *etcdBakedLoadBalancer) setPauseSettle() {
	l.setStop(true)
	l.lbSettleChan <- time.Now().UTC()
}

func (l *etcdBakedLoadBalancer) monitorLbPause() {
	stopped := l.stopped
	changed := false
	for {
		select {
		case t := <-l.lbSettleChan:
			changed = true
			stopped = true
			now := time.Now().UTC()
			lastUpdate := t
			if lastUpdate.Add(l.settleTime).After(now) {
				runIn := lastUpdate.Add(l.settleTime).Sub(now)
				l.logger.Debug("waiting for settle time : ", runIn)
				time.Sleep(runIn)
			}
			stopped = false
		case cmd := <-l.lbPauseChan:
			changed = true
			if cmd == pauseCmd {
				stopped = true
			} else {
				stopped = false
			}
		case <-l.ctx.Done():
			l.logger.Debug("exiting the lb pause monitor")
			return
		case <-time.After(time.Second * 3):
			if changed {
				l.logger.Debug("setting pause to ", stopped)
				l.setStop(stopped)
			}
			changed = false
		}
	}
}

package lb

import (
	"context"
	"errors"
	"fmt"
	"github.com/BeameryHQ/async-stream/logging"
	"github.com/BeameryHQ/async-stream/metrics"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/BeameryHQ/async-stream/util"
	"github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
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

type etcdBackedLoadBalancer struct {
	ctx            context.Context
	cancelFunc     context.CancelFunc
	logger         *logrus.Entry
	cli            *clientv3.Client
	path           string
	ring           *hashring.HashRing
	myTarget       string
	ringLock       *sync.Mutex
	cache          map[string]bool
	notifyChan     chan *LbEvent
	notifyBulkChan chan []*LbEvent
	settleTime     time.Duration
	stopped        bool
	stoppedMu      *sync.Mutex
	lbPauseChan    chan string
	lbSettleChan   chan *LbEvent
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
	notifyBulkChan := make(chan []*LbEvent)

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	l := &etcdBackedLoadBalancer{
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
		notifyChan:     notifyChan,
		notifyBulkChan: notifyBulkChan,
		settleTime:     config.settleTime,
		stoppedMu:      &sync.Mutex{},
		lbPauseChan:    make(chan string, 100),
		lbSettleChan:   make(chan *LbEvent, 100),
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

	metrics.SetOnline()

	return l, nil
}

func (l *etcdBackedLoadBalancer) Target(key string, waitSettleTime bool) (string, error) {
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

func (l *etcdBackedLoadBalancer) Notify() <-chan *LbEvent {
	return l.notifyChan
}

func (l *etcdBackedLoadBalancer) NotifyBulk() <-chan []*LbEvent {
	return l.notifyBulkChan
}

func (l *etcdBackedLoadBalancer) Close() {
	l.logger.Debug("closing the lb by calling Close")
	l.cancelFunc()
}

// should only be called inside of concurrently safe methods
func (l *etcdBackedLoadBalancer) sendNotification(e *LbEvent) {
	select {
	case l.notifyChan <- e:
		l.logger.Debugln("send notification : ", e)
	default:
		return
	}
}

// sendBulkNotification is clever version of sendNotification where one addition of lb cancels out one removal
// also if there's one stopped event the others are discarded and are sent in bulk to downstream
func (l *etcdBackedLoadBalancer) sendBulkNotification(events []*LbEvent) {
	if len(events) == 0 {
		return
	}
	// try to send only what matters
	var finalEvents []*LbEvent

	send := func() {
		select {
		case l.notifyBulkChan <- finalEvents:
			l.logger.Debugf("send bulk notification : %s", spew.Sdump(finalEvents))
		default:
			return
		}
	}

	added := []*LbEvent{}
	addedSet := map[string]bool{}

	removed := []*LbEvent{}
	removedSet := map[string]bool{}

	for _, e := range events {
		if e.Event == LbStopped {
			finalEvents = []*LbEvent{e}
			send()
			return
		}

		if e.Event == TargetAdded {
			if !addedSet[e.Target] {
				added = append(added, e)
				addedSet[e.Target] = true
			}
		}

		if e.Event == TargetRemoved {
			if !removedSet[e.Target] {
				removed = append(removed, e)
				removedSet[e.Target] = true
			}
		}
	}

	// there's no need to do anything at this point
	if len(added) == len(removed) {
		return
	}

	if len(added) > len(removed) {
		finalEvents = added[len(removed):]
	} else {
		finalEvents = removed[len(added):]
	}

	send()
}

func (l *etcdBackedLoadBalancer) getNodeSafe(key string) (string, error) {
	l.ringLock.Lock()
	defer l.ringLock.Unlock()

	target, ok := l.ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("no target found %s", target)
	}

	return target, nil
}

func (l *etcdBackedLoadBalancer) registerTarget(target string) error {
	l.ringLock.Lock()
	defer l.ringLock.Unlock()

	l.setPauseSettle(NewAddedEvent(target))
	if l.cache[target] {
		return nil
	}

	l.cache[target] = true

	l.ring = l.ring.AddNode(target)

	l.logger.Info("registered a new target : ", target, l.cache)
	return nil
}

func (l *etcdBackedLoadBalancer) removeTarget(target string) error {
	l.ringLock.Lock()
	defer l.ringLock.Unlock()

	l.setPauseSettle(NewRemovedEvent(target))
	if target == l.myTarget {
		return nil
	}

	delete(l.cache, target)
	l.ring = l.ring.RemoveNode(target)

	l.logger.Info("removed a target : ", target, l.cache)
	return nil
}

func (l *etcdBackedLoadBalancer) addTargetRemote(ctx context.Context, target string) error {
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
					err = util.RetryNormal(func() error {
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

	return nil
}

func (l *etcdBackedLoadBalancer) getKeepAliveChan(ctx context.Context, key string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
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

func (l *etcdBackedLoadBalancer) extractTargetFromKey(key string) (string, error) {
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

func (l *etcdBackedLoadBalancer) keyHandler(event *stream.FlowEvent) error {

	target, err := l.extractTargetFromKey(string(event.Kv.Key))
	if err != nil {
		return err
	}

	return l.registerTarget(target)
}

func (l *etcdBackedLoadBalancer) watchHandler(event *stream.FlowEvent) error {
	target, err := l.extractTargetFromKey(string(event.Kv.Key))
	if err != nil {
		return err
	}

	if event.IsCreated() || event.IsUpdated() {
		return l.registerTarget(target)
	}

	return l.removeTarget(target)
}

func (l *etcdBackedLoadBalancer) isStopped() (bool, error) {
	l.stoppedMu.Lock()
	defer l.stoppedMu.Unlock()

	if l.ctx.Err() != nil {
		l.logger.Errorf("lb is down, can't continue %v", l.ctx.Err())
		return false, DownErr
	}

	return l.stopped, nil
}

func (l *etcdBackedLoadBalancer) setStop(val bool) {
	l.stoppedMu.Lock()
	defer l.stoppedMu.Unlock()
	if val {
		metrics.SetOffline()
	} else {
		metrics.SetOnline()
	}

	l.stopped = val
}

func (l *etcdBackedLoadBalancer) setPause() {
	l.setStop(true)
	l.lbPauseChan <- pauseCmd
}

func (l *etcdBackedLoadBalancer) clearPause() {
	l.lbPauseChan <- unpauseCmd
}

func (l *etcdBackedLoadBalancer) setPauseSettle(event *LbEvent) {
	l.setStop(true)
	l.lbSettleChan <- event
}

func (l *etcdBackedLoadBalancer) monitorLbPause() {
	var stopped, changed, paused bool
	var lbEvents []*LbEvent
	for {
		select {
		case e := <-l.lbSettleChan:
			changed = true
			stopped = true
			now := time.Now().UTC()
			lastUpdate := e.CreatedOn
			if lastUpdate.Add(l.settleTime).After(now) {
				runIn := lastUpdate.Add(l.settleTime).Sub(now)
				l.logger.Debug("waiting for settle time : ", runIn)
				time.Sleep(runIn)
			}

			// send the event
			if e.Target != l.myTarget {
				l.sendNotification(e)
				lbEvents = append(lbEvents, e)
			}

			if !paused {
				stopped = false
			}
		case cmd := <-l.lbPauseChan:
			changed = true
			if cmd == pauseCmd {
				stopped = true
				paused = true
			} else {
				stopped = false
				paused = false
			}
		case <-l.ctx.Done():
			changed = true
			stopped = true
			l.logger.Info("exiting the lb pause monitor")
			lbEvents = append(lbEvents, NewLbStoppedEvent())
			l.sendBulkNotification(lbEvents)

			//close the chan so downstream can react on that if missed the message
			close(l.notifyChan)
			close(l.notifyBulkChan)

			return

		case <-time.After(time.Second * 3):
			if changed {
				l.logger.Debug("setting pause to ", stopped)
				l.setStop(stopped)
				l.sendBulkNotification(lbEvents)
				lbEvents = []*LbEvent{}
			}
			changed = false
		}
	}
}

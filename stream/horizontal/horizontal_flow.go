package horizontal

import (
	"context"
	"github.com/BeameryHQ/async-stream/lb"
	"github.com/BeameryHQ/async-stream/stream"
	"github.com/Sirupsen/logrus"
	"k8s.io/client-go/util/workqueue"
	"strings"
	"sync"
)

// Flow is same as Flow but makes sure the handlers get only partitioned data
// it's not able to distinguish between the the list and watch because of the nature of other members
// joining and leaving might need to process the data of an old member who just left
type Flow interface {
	RegisterHandler(h stream.FlowEventHandler)
	Run(ctx context.Context, block bool) error
}

type FlowProcessorProvider struct {
	// if enabled the list handlers won't be registered
	fromEnd      bool
	cancel       context.CancelFunc
	flow         stream.Flow
	lb           lb.KeyLbNotifier
	path         string
	handlers     []stream.FlowEventHandler
	consumerName string
	cache        *flowCache
	logger       *logrus.Entry
	queue        workqueue.Interface
}

// only can be run in the beginning not dynamic
func (f *FlowProcessorProvider) RegisterHandler(h stream.FlowEventHandler) {
	f.handlers = append(f.handlers, h)
}

func (f *FlowProcessorProvider) Run(ctx context.Context, block bool) error {
	ctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel

	if !f.fromEnd {
		f.flow.RegisterListHandler(f.path, f.listHandler)
	}
	f.flow.RegisterWatchHandler(f.path, f.watchHandler)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer cancel()

		f.logger.Info("starting the flow processor on path ", f.path)
		f.flow.Run(ctx)
	}()

	go f.monitorLbChanges(ctx)
	go f.processQueue(cancel)

	if !block {
		return nil
	}

	wg.Wait()
	cancel()

	return nil
}

func (f *FlowProcessorProvider) processQueue(cancel context.CancelFunc) {
	defer cancel()
	for {
		if err := f.processQueueItem(); err != nil {
			if err == stream.ErrFlowTerminated {
				f.logger.Warnf("exiting processing got exit message : %v", err)
				return
			}
			f.logger.Errorf("processing key failed : %v", err)
		}
	}
}

func (f *FlowProcessorProvider) processQueueItem() error {
	key, exit := f.queue.Get()
	defer f.queue.Done(key)
	if exit {
		f.logger.Warningf("queue got shutdown signal exiting")
		return stream.ErrFlowTerminated
	}

	event := f.cache.get(key.(string))
	if event == nil {
		f.logger.Warningf("expired key %s", key.(string))
		return nil
	}

	if err := f.lbStreamHandler(event); err != nil {
		return err
	}

	return nil
}

func (f *FlowProcessorProvider) listHandler(event *stream.FlowEvent) error {
	if f.handlers == nil || len(f.handlers) == 0 {
		return nil
	}

	if f.shouldSkipPath(event.Kv.Key) {
		return nil
	}

	f.cache.put(event.Kv.Key, event)

	ok, err := f.isEventForMe(event)
	if err != nil || !ok {
		return err
	}
	f.queue.Add(event.Kv.Key)
	return nil
}

func (f *FlowProcessorProvider) watchHandler(event *stream.FlowEvent) error {
	if f.handlers == nil || len(f.handlers) == 0 {
		return nil
	}

	if f.shouldSkipPath(event.Kv.Key) {
		return nil
	}

	if event.IsDeleted() {
		f.cache.del(event.Kv.Key)
		return nil
	} else {
		f.cache.put(event.Kv.Key, event)
	}

	ok, err := f.isEventForMe(event)
	if err != nil || !ok {
		return err
	}

	f.queue.Add(event.Kv.Key)
	return nil
}

func (f *FlowProcessorProvider) isEventForMe(event *stream.FlowEvent) (bool, error) {
	target, err := f.lb.Target(event.Kv.Key, true)
	if err != nil {
		if err == lb.DownErr {
			f.cancel()
			return false, stream.ErrFlowTerminated
		}
		return false, err
	}

	// skip this one
	if target != f.consumerName {
		return false, nil
	}

	return true, nil
}

func (f *FlowProcessorProvider) lbStreamHandler(event *stream.FlowEvent) error {

	ok, err := f.isEventForMe(event)
	if err != nil || !ok {
		return err
	}

	f.logger.Debug("lbStreamHandler processing event : ", event.Kv.Key)
	for _, c := range f.handlers {
		if err := c(event); err != nil {
			return err
		}
	}

	return nil
}

// checks the dirty flag and does the re-processing of the whole cache again
func (f *FlowProcessorProvider) reProcessCache() {
	f.logger.Info("re processing the cache from the beginning ")
	i := 0
	for v := range f.cache.iterate() {
		f.queue.Add(v.Kv.Key)
		i++
	}
	f.logger.Infof("%d items were re-queued for processing again", i)
}

func (f *FlowProcessorProvider) monitorLbChanges(ctx context.Context) {
	defer f.cancel()
	ch := f.lb.NotifyBulk()
	for {
		select {
		case events, ok := <-ch:
			if !ok {
				f.logger.Info("lb channel is closed exiting")
				return
			}
			for _, e := range events {
				if e.Target == lb.LbStopped {
					f.logger.Info("got lb shutdown message going to trigger exit")
					return
				}

				if e.Event == lb.TargetRemoved {
					f.logger.Info("lb change need to re-shuffle")
					f.reProcessCache()
					break
				}
			}
		case <-ctx.Done():
			f.logger.Warningf("exiting lb monitor, ctx signal")
			return
		}
	}
}

func (f *FlowProcessorProvider) shouldSkipPath(path string) bool {
	parentPath := strings.TrimRight(f.path, "/")
	parentPath = parentPath + "/lb"

	return strings.HasPrefix(path, parentPath)
}

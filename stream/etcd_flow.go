package stream

import (
	"context"
	"github.com/BeameryHQ/async-stream/logging"
	"github.com/BeameryHQ/async-stream/metrics"
	"github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"sync"
)

const (
	defaultWatchPathBuffer = 1000
	defaultPageSize        = 30
)

type EtcdFlow struct {
	config        *flowConfig
	logger        *logrus.Entry
	watchHandlers map[string][]FlowEventHandler
	watchBufChans map[string]chan *clientv3.Event
	keyHandlers   map[string][]FlowEventHandler
	cli           *clientv3.Client
	// a map of chans which helps to start list processing at the right time
	readyWatchChan map[string]chan bool
}

type flowConfig struct {
	watchBufferSize int
	pageSize        int
}

func newFlowConfig() *flowConfig {
	return &flowConfig{
		watchBufferSize: defaultWatchPathBuffer,
		pageSize:        defaultPageSize,
	}
}

type FlowOption func(config *flowConfig)

func WithWatchBufferSize(size int) FlowOption {
	return func(config *flowConfig) {
		config.watchBufferSize = size
	}
}

func WithPageSize(size int) FlowOption {
	return func(config *flowConfig) {
		config.pageSize = size
	}
}

func NewEtcdFlow(cli *clientv3.Client, opts ...FlowOption) Flow {
	logger := logging.GetLogger().WithFields(
		logrus.Fields{"component": "flow",
		},
	)

	config := newFlowConfig()
	for _, o := range opts {
		o(config)
	}

	return &EtcdFlow{
		config:         config,
		logger:         logger,
		watchHandlers:  map[string][]FlowEventHandler{},
		keyHandlers:    map[string][]FlowEventHandler{},
		watchBufChans:  map[string]chan *clientv3.Event{},
		readyWatchChan: map[string]chan bool{},
		cli:            cli,
	}
}

func (f *EtcdFlow) RegisterWatchHandler(path string, h FlowEventHandler) {
	f.readyWatchChan[path] = make(chan bool)

	if f.watchHandlers[path] == nil {
		f.watchHandlers[path] = []FlowEventHandler{
			h,
		}
		return
	}

	f.watchHandlers[path] = append(f.watchHandlers[path], h)
}

func (f *EtcdFlow) RegisterListHandler(path string, h FlowEventHandler) {
	if f.keyHandlers[path] == nil {
		f.keyHandlers[path] = []FlowEventHandler{
			h,
		}
		return
	}

	f.keyHandlers[path] = append(f.keyHandlers[path], h)
}

func (f *EtcdFlow) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	f.logger.Debug("watch handlers : ", f.watchHandlers)
	f.logger.Debug("key handlers : ", f.keyHandlers)

	barriers := len(f.watchHandlers)
	var wg sync.WaitGroup
	if barriers > 0 {
		wg.Add(barriers)
	}

	for p, hs := range f.watchHandlers {
		f.watchBufChans[p] = make(chan *clientv3.Event, f.config.watchBufferSize)

		go func(path string, handlers []FlowEventHandler) {
			defer wg.Done()
			wchan := f.cli.Watch(ctx, path, clientv3.WithPrefix())
			f.logger.Debugln("creating a watch channel on path : ", path)

			// if there's a list handler on than path signal it
			if f.keyHandlers[path] != nil {
				logrus.Debug("waiting for list handler to start ", path)
				f.readyWatchChan[path] <- true
			}

			for {
				select {
				case msg, ok := <-wchan:
					if !ok {
						f.logger.Warning("the watch channel is closed exiting for path : ", path)
						return
					}
					// put them inside the path buffer so we don't get bitten by slow consumers
					for _, event := range msg.Events {
						f.watchBufChans[path] <- event
					}
				case event := <-f.watchBufChans[path]:
					for _, handler := range handlers {
						e := flowEventFromEtcd(event)
						f.incrEventMetrics(e, true)
						err := handler(e)
						if err != nil {
							metrics.IncrFlowFailed()
							f.logger.Errorf("handler failed : %v", err)
						} else {
							metrics.IncrFlowProcessed()
						}

						if err == ErrFlowTerminated {
							f.logger.Warnf("exit error received exiting : %v", err)
							cancel()
							return
						}
					}

				case <-ctx.Done():
					f.logger.Debug("exiting handler for path ", path)
					return
				}
			}

		}(p, hs)

	}
	f.logger.Debug("watch handlers started", len(f.watchHandlers))
	// first we do the key handlers and then start the
	// async watch handlers
	for p, hs := range f.keyHandlers {
		// wait for the watcher to start so we dont miss any events
		//f.logger.Println("ready chans", f.readyWatchChan)
		if f.readyWatchChan[p] != nil {
			f.logger.Debug("waiting for the chan to send watch ready ", p)
			<-f.readyWatchChan[p]
		}

		exiting := false
		for _, h := range hs {
			f.logger.Debug("starting processing path for list handlers ", p, h)
			if err := f.fetchProcessKeys(ctx, p, h); err != nil {
				f.logger.Errorf("failed processing path with key handler %s : %v", p, err)
				if err == ErrFlowTerminated{
					cancel()
					exiting = true
					break
				}
			}
		}

		if exiting {
			break
		}
	}

	wg.Wait()
}

func (f *EtcdFlow) fetchProcessKeys(ctx context.Context, path string, handler FlowEventHandler) error {

	countResp, err := f.cli.Get(ctx, path, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return err
	}

	f.logger.Debugf("found %d items on path : %s, starting the processing", countResp.Count, path)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(int64(f.config.pageSize)),
	}

	firstFetch := true
	currentPath := path
	for {
		resp, err := f.cli.Get(
			ctx,
			currentPath,
			opts...,
		)
		if err != nil {
			return err
		}

		if firstFetch {
			opts = append(opts, clientv3.WithFromKey(), clientv3.WithRange(path+"/x00"))
		}

		keys := resp.Kvs
		if len(keys) == 0 {
			break
		}

		if !firstFetch {
			if len(keys) == 1 {
				break
			}
			keys = keys[1:]
		}
		currentPath = string(keys[len(keys)-1].Key)

		for _, k := range keys {
			e := flowEventFromEtcdListKey(k)
			f.incrEventMetrics(e, false)
			err := handler(e)
			if err != nil {
				metrics.IncrFlowFailed()
				f.logger.Printf("skipping key in key handler %s : %v", k, err)
				if err == ErrFlowTerminated {
					return err
				}
				continue
			}
			metrics.IncrFlowProcessed()

		}

		firstFetch = false

	}

	return nil
}

func (f *EtcdFlow) incrEventMetrics(e *FlowEvent, watch bool) {
	switch e.Type {
	case FlowEventCreated:
		if !watch {
			metrics.IncrFlowCreatedList()
		} else {
			metrics.IncrFlowCreatedWatch()
		}
	case FlowEventUpdated:
		metrics.IncrFlowUpdated()
	case FlowEventDeleted:
		metrics.IncrFlowDeleted()
	}
}

func flowEventFromEtcd(e *clientv3.Event) *FlowEvent {
	if e == nil {
		return nil
	}
	var eventType FlowEventType

	if e.IsModify() {
		eventType = FlowEventUpdated
	} else if e.IsCreate() {
		eventType = FlowEventCreated
	} else {
		eventType = FlowEventDeleted
	}

	return &FlowEvent{
		Type:   eventType,
		Kv:     flowKeyValueFromEtcd(e.Kv),
		PrevKv: flowKeyValueFromEtcd(e.PrevKv),
	}

}

func flowEventFromEtcdListKey(kv *mvccpb.KeyValue) *FlowEvent {
	if kv == nil {
		return nil
	}

	return &FlowEvent{
		Type:   FlowEventCreated,
		Kv:     flowKeyValueFromEtcd(kv),
		PrevKv: nil,
	}
}

func flowKeyValueFromEtcd(kv *mvccpb.KeyValue) *FlowKeyValue {
	if kv == nil {
		return nil
	}

	return &FlowKeyValue{
		Key:            string(kv.Key),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Value:          string(kv.Value),
	}
}

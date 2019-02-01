package stream

import (
	"context"
	"github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/SeedJobs/async-stream/logging"
	"go.etcd.io/etcd/clientv3"

	"sync"
)

const (
	watchPathBuffer = 1000
)

type EtcdFlow struct {
	logger        *logrus.Entry
	watchHandlers map[string][]FlowEventHandler
	watchBufChans map[string]chan *clientv3.Event
	keyHandlers   map[string][]FlowEventHandler
	cli           *clientv3.Client
	// a map of chans which helps to start list processing at the right time
	readyWatchChan map[string]chan bool
}

func NewEtcdFlow(cli *clientv3.Client) Flow {
	logger := logging.GetLogger().WithFields(
		logrus.Fields{"component": "flow",
		},
	)
	return &EtcdFlow{
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
	f.logger.Debug("watch handlers : ", f.watchHandlers)
	f.logger.Debug("key handlers : ", f.keyHandlers)

	barriers := len(f.watchHandlers)
	var wg sync.WaitGroup
	wg.Add(barriers)

	for p, hs := range f.watchHandlers {
		if err := f.createPathIfNotExists(ctx, p); err != nil {
			f.logger.Fatal("creating the path failed ", err)
		}

		f.watchBufChans[p] = make(chan *clientv3.Event, watchPathBuffer)

		go func(path string, handlers []FlowEventHandler) {
			defer wg.Done()
			wchan := f.cli.Watch(ctx, path, clientv3.WithPrefix())
			f.logger.Debugln("creating a watch channel on path : ", path)

			// if there's a list handler on than path signal it
			if f.keyHandlers[path] != nil{
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
						err := handler(flowEventFromEtcd(event))
						if err != nil {
							f.logger.Errorf("handler failed : %v", err)
						}
					}

				case <-ctx.Done():
					f.logger.Debug("exiting handler for path ", path)
					return
				}
			}

		}(p, hs)

	}

	// first we do the key handlers and then start the
	// async watch handlers
	for p, hs := range f.keyHandlers {
		if err := f.createPathIfNotExists(ctx, p); err != nil {
			f.logger.Fatal("creating the path failed ", err)
		}

		// wait for the watcher to start so we dont miss any events
		//f.logger.Println("ready chans", f.readyWatchChan)
		if f.readyWatchChan[p] != nil{
			f.logger.Debug("waiting for the chan to send watch ready ", p)
			<- f.readyWatchChan[p]
		}

		for _, h := range hs {
			if err := f.fetchProcessKeys(ctx, p, h); err != nil {
				f.logger.Fatalf("failed processing path with key handler %s : %v", p, err)
			}
		}
	}



	wg.Wait()
}

func (f *EtcdFlow) fetchProcessKeys(ctx context.Context, path string, handler FlowEventHandler) error {
	resp, err := f.cli.Get(ctx, path, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, k := range resp.Kvs {
		err := handler(flowEventFromEtcdListKey(k))
		if err != nil {
			f.logger.Printf("skipping key in key handler %s : %v", k, err)
			continue
		}
	}

	return nil
}

func (f *EtcdFlow) createPathIfNotExists(ctx context.Context, path string) error {
	//f.logger.Println("running createPath for : ", path)
	//resp, err := f.cli.Get(ctx, path, clientv3.WithCountOnly())
	//if err != nil {
	//	return err
	//}
	//
	//if resp.Count > 0 {
	//	return nil
	//}
	//
	//_, err = f.cli.Put(ctx, path, "")
	//if err != nil {
	//	return err
	//}

	return nil
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

## Asynchronous Stream Processing Utilities

Project's aim is to supply convenient utilities for stream processing. Currently they work only against [etcd](https://github.com/etcd-io/etcd).

 - `stream`: package contains an interface for registering event processors for certain paths so, the user
 doesn't have to deal with low level details of the backing store.

 - `lb`: has implementation backed by etcd that helps with load balancing. Given a certain routing key it helps to
 find the target and also handles the dynamic targets being added and removed from the system.

 - `jobs`: has an implementation of worker system that uses `stream` and `lb` packages.
 Currently it works against [etcd](https://github.com/etcd-io/etcd) and helps with processing of background jobs.



#### Installation


  - `go get github.com/BeameryHQ/async-stream/jobs`
  - `go get github.com/BeameryHQ/async-stream/stream`
  - `go get github.com/BeameryHQ/async-stream/kvstore`
  - `go get github.com/BeameryHQ/async-stream/lb`

#### Stream Processing

`stream.Flow` interface allows to register different kinds of event handlers.
The main two methods are :

```golang
RegisterWatchHandler(path string, handler FlowEventHandler)
RegisterListHandler(path string, handler FlowEventHandler)
```

- `RegisterListHandler` : is useful for cases where user needs to process the stream from the beginning. It runs synchronously
if it's the only handler type registered the Flow interface runs till the end of the stream. In etcd terms these handlers
will process whatever comes from the `GetRange` requests for the given path.
Multiple handlers can be registered on the same path in which case they will be run serially.


- `RegisterWatchHandler`: is same as the `RegisterListHandler` except it's interested in real time updates on the stream. In terms of
etcd it uses it's `Watch` functionality.

If there's a need to get all the updates from the beginning of the stream and continue processing as the changes come List and Watch
handlers should be registered together.


```golang

cli, err := clientv3.New(clientv3.Config{
    Endpoints:   []string{"localhost:2379"},
	DialTimeout: 5 * time.Second,
})

if err != nil {
    log.Fatalf("etcd client creation failed %v", err)
}

f := stream.NewEtcdFlow(cli)
f.RegisterListHandler("/async", func(event *stream.FlowEvent) error {
	fmt.Println("got a key from list handler : ", event.Kv.Key)
	fmt.Println("key handler value is : ", event.Kv.Value)
	return nil
})
f.RegisterWatchHandler("/async", func(event *stream.FlowEvent) error {
    fmt.Println("got a new event ", event)
    fmt.Printf("the key is %s ", string(event.Kv.Key))
    fmt.Println("the value is : ", string(event.Kv.Value))
    return nil
})
f.Run(context.Background())
```


#### Dynamic Key Routing

Currently it's backed by etcd and uses it's lease Api to register the LB targets under a given path.
Also it uses the `stream.Flow` interface to be able to react on stream changes. For example, when a new
target is added or removed its update its cached targets. Internally it is using consistent hashing to make
sure the same key goes to the same target on all instances. Because of the nature of things being distributed
there's no guarantee that same key won't go to the same instance, it can happen during re-balancing. The client side
should make sure it's handling that scenario.

`lb.KeyLbNotifier` has 2 important methods :

- `Target` : given a routing key returns the target according to the local cache
- `Notify` : returns notification changes for the underlying target changes. For example,
`jobs.StreamConsumer` uses that information to stop the processing and re-submit the jobs from local cache
in case a certain job was re-assigned to current consumer instance.


#### Background Job Processing

`jobs.StreamConsumer` provides convenient abstractions for async background processing. Some of the features are :

- Listen for certain stream path and process jobs
- Being able to get only those jobs that are related to current consumer instance.
- Restarting the jobs as they fail with exponential back off and upper limit.
- Saving progress of a job in case it's a long running one so can be resumed in case of failure.
- Internal in-memory worker system that allows parallelizing jobs as much as possible.
- Registering different tasks per consumer which helps with grouping jobs that might need similar needs.



For more detailed examples check the [examples](https://github.com/BeameryHQ/async-stream/tree/master/examples) directory.

package metrics

const (
	processedKey   = "flow_items_processed"
	failedKey      = "flow_items_failed"
	createdKey     = "flow_items_created"
	createdListKey = "flow_items_created_list"
	createdWatcKey = "flow_items_created_watch"
	updatedKey     = "flow_items_updated"
	deletedKey     = "flow_items_deleted"
)

var (
	flowProcessed    = newSimpleMetric(processedKey)
	flowFailed       = newSimpleMetric(failedKey)
	flowCreated      = newSimpleMetric(createdKey)
	flowCreatedList  = newSimpleMetric(createdListKey)
	flowCreatedWatch = newSimpleMetric(createdWatcKey)
	flowUpdated      = newSimpleMetric(updatedKey)
	flowDeleted      = newSimpleMetric(deletedKey)
)

func IncrFlowProcessed() {
	flowProcessed.Incr()
}

func IncrFlowFailed() {
	flowFailed.Incr()
}

func IncrFlowCreated() {
	flowCreated.Incr()
}

func IncrFlowCreatedList() {
	flowCreatedList.Incr()
}

func IncrFlowCreatedWatch() {
	flowCreatedWatch.Incr()
}

func IncrFlowUpdated() {
	flowUpdated.Incr()
}

func IncrFlowDeleted() {
	flowDeleted.Incr()
}

package metrics

const (
	processedKey = "items_processed"
	failedKey    = "items_failed"
	createdKey   = "items_created"
	updatedKey   = "items_updated"
	deletedKey   = "items_deleted"
)

var (
	flowProcessed *simpleMetric
	flowFailed    *simpleMetric
	flowCreated   *simpleMetric
	flowUpdated   *simpleMetric
	flowDeleted   *simpleMetric
)

func init() {
	flowProcessed = newSimpleMetric(processedKey)
	flowFailed = newSimpleMetric(failedKey)
	flowCreated = newSimpleMetric(createdKey)
	flowUpdated = newSimpleMetric(updatedKey)
	flowDeleted = newSimpleMetric(deletedKey)
}

func IncrFlowProcessed() {
	flowProcessed.Incr()
}

func IncrFlowFailed() {
	flowFailed.Incr()
}

func IncrFlowCreated() {
	flowCreated.Incr()
}

func IncrFlowUpdated() {
	flowUpdated.Incr()
}

func IncrFlowDeleted() {
	flowDeleted.Incr()
}

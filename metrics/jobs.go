package metrics

const (
	jobsFinishedKey     = "jobs_finished"
	jobsErroredKey      = "jobs_errored"
	jobsFailedKey       = "jobs_failed"
	jobsRunningCountKey = "jobs_running"
	jobsRunningAvgKey   = "jobs_running_avg"
)

var (
	jobsFinished = newSimpleMetric(jobsFinishedKey)
	jobsErrored  = newSimpleMetric(jobsErroredKey)
	jobsFailed   = newSimpleMetric(jobsFailedKey)
)

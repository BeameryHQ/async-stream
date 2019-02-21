package metrics

import (
	"expvar"
	"github.com/paulbellamy/ratecounter"
	"time"
)

const (
	jobsFinishedKey      = "jobs_finished"
	jobsErroredKey       = "jobs_errored"
	jobsFailedKey        = "jobs_failed"
	jobsRunningCountKey  = "jobs_running"
	jobsRunningAvgMsKey  = "jobs_running_avg_ms"
	jobsRunningAvgSecKey = "jobs_running_avg_sec"
)

var (
	jobsFinished   = newSimpleMetric(jobsFinishedKey)
	jobsErrored    = newSimpleMetric(jobsErroredKey)
	jobsFailed     = newSimpleMetric(jobsFailedKey)
	jobsRunningAvg = ratecounter.NewAvgRateCounter(60 * time.Second)
)

func IncrJobsFinished() {
	jobsFinished.Incr()
}

func IncrJobsErrored() {
	jobsErrored.Incr()
}

func IncrJobsFailed() {
	jobsFailed.Incr()
}

func IncrJobsRunningElapsed(cb func()) {
	// Start timer.
	startTime := time.Now()
	// Execute heavy operation.

	counts.Add(jobsRunningCountKey, 1)
	defer counts.Add(jobsRunningCountKey, -1)
	cb()

	// Record elapsed time.
	jobsRunningAvg.Incr(int64(time.Since(startTime) / time.Millisecond))
	// Get the currentMs average execution time.
	avg := jobsRunningAvg.Rate()

	currentMs := new(expvar.Float)
	currentMs.Set(avg)

	currentSec := new(expvar.Float)
	currentSec.Set(avg / 1000)

	counts.Set(jobsRunningAvgMsKey, currentMs)
	counts.Set(jobsRunningAvgSecKey, currentSec)

}

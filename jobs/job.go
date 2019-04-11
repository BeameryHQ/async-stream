package jobs

import (
	"github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"time"
)

const defaultMaxJobRetries = 6

type JobParameters map[string]interface{}

type Job struct {
	Id           string        `json:"Id"`
	TaskName     string        `json:"TaskName"`
	Args         JobParameters `json:"Args"`
	State        string        `json:"State"`
	CreatedAt    time.Time     `json:"CreatedAt"`
	UpdatedAt    time.Time     `json:"UpdatedAt"`
	Result       string        `json:"Result"`
	MaxRetry     int64         `json:"MaxRetry"`
	CurrentRetry int64         `json:"CurrentRetry"`
	Errors       []string      `json:"Errors"`
	// attached to the job when picked up by the consumer
	ConsumerName string        `json:"ConsumerName"`
}

func NewJob(taskName string, args JobParameters) (string, *Job) {
	return NewJobWithRetry(taskName, args, defaultMaxJobRetries)
}

func NewJobWithRetry(taskName string, args JobParameters, maxRetry int) (string, *Job) {
	jobId := uuid.NewV4().String()
	j := &Job{
		Id:        jobId,
		TaskName:  taskName,
		Args:      args,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		MaxRetry:  int64(maxRetry),
	}

	return jobId, j
}

func (j *Job) IsFailed() bool {
	return j.CurrentRetry >= j.MaxRetry
}

type JobHandler interface {
	Handle(jobStore JobStore, j *Job, logger *logrus.Entry) error
}

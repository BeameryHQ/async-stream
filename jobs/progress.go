package jobs

import (
	"context"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	defaultProgressTick = 15 * time.Second
	defaultTimeout      = 15 * time.Second
)

type progressOption struct {
	tickTime time.Duration
}

func newProgressOption() *progressOption {
	return &progressOption{
		tickTime: defaultProgressTick,
	}
}

type RunOption func(*progressOption)

func WithTickTime(t time.Duration) RunOption {
	return func(option *progressOption) {
		if t != 0 {
			option.tickTime = t
		}
	}
}

type Progress struct {
	jobStore JobStore
	logger   *logrus.Entry
}

func NewProgress(jobStore JobStore, logger *logrus.Entry) *Progress {
	return &Progress{
		jobStore: jobStore,
		logger:   logger,
	}
}

func (p *Progress) saveProgress(ctx context.Context, jobId string, progress interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	return p.jobStore.SaveResult(ctx, jobId, progress)
}

func (p *Progress) RunWithProgressResult(ctx context.Context, jobId string, slowFunc func(progressChan chan<- interface{}) error) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	progressChan := make(chan interface{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer cancel()
		for {
			select {
			case progress, ok := <-progressChan:
				if !ok {
					return
				}

				err := p.saveProgress(ctx, jobId, progress)
				if err != nil {
					p.logger.Warningf("saving the Progress failed : %v", err)
				}
			case <-ctx.Done():
				p.logger.Debug("exiting progress chan loop", jobId)
				return
			}

		}
	}()

	slowErr := slowFunc(progressChan)
	cancel()
	wg.Wait()

	return slowErr
}

func (p *Progress) RunWithProgressTime(ctx context.Context, jobId string, slowFunc func() error, opts ...RunOption) error {
	c := newProgressOption()
	for _, o := range opts {
		o(c)
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tickTime := c.tickTime
		lastTimeElapsed := time.Now().UTC()

		tick := time.NewTicker(tickTime).C
		for {
			select {
			case <-tick:
				now := time.Now().UTC()
				diff := now.Sub(lastTimeElapsed)
				lastTimeElapsed = now
				p.logger.Debug("sending touch command for : ", jobId, diff)
				if err := p.jobStore.Touch(ctx, jobId); err != nil {
					p.logger.Warnf("sending the touch command failed %v", err)
				}
			case <-ctx.Done():
				p.logger.Debug("exiting the progress tickera", jobId)
				return
			}
		}
	}()

	err := slowFunc()
	cancel()
	wg.Wait()
	p.logger.Debug("slow func done processing ", jobId)
	return err
}

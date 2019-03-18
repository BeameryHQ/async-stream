package stream

import (
	"context"
	"errors"
)
var (
	ErrFlowTerminated = errors.New("flow terminated")
)
type FlowEventHandler func(event *FlowEvent) error

type Flow interface {
	RegisterWatchHandler(path string, handler FlowEventHandler)
	RegisterListHandler(path string, handler FlowEventHandler)
	Run(ctx context.Context)
}



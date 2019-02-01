package stream

type FlowEventType string

const (
	FlowEventCreated FlowEventType = "CREATED"
	FlowEventUpdated FlowEventType = "UPDATED"
	FlowEventDeleted FlowEventType = "DELETED"
)

type FlowKeyValue struct {
	Key            string
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Value          string
}

type FlowEvent struct {
	Type   FlowEventType
	Kv     *FlowKeyValue
	PrevKv *FlowKeyValue
}

func (e *FlowEvent) IsCreated() bool {
	return e.Type == FlowEventCreated
}

func (e *FlowEvent) IsUpdated() bool {
	return e.Type == FlowEventUpdated
}

func (e *FlowEvent) IsDeleted() bool {
	return e.Type == FlowEventDeleted
}

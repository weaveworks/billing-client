package client

import (
	"time"
)

// Event is a record of some amount of billable usage for scope.
type Event struct {
	UniqueKey          string            `json:"unique_key" msg:"unique_key"`
	InternalInstanceID string            `json:"internal_instance_id" msg:"internal_instance_id"`
	OccurredAt         time.Time         `json:"occurred_at" msg:"occurred_at"`
	Amounts            Amounts           `json:"amounts" msg:"amounts"`
	Metadata           map[string]string `json:"metadata" msg:"metadata"`
}

// msgpack (and therefore fluentd) requires the things we send to it to be
// map[string]interface{}, so we return them here, not a struct. :(
func (e Event) toRecords() []map[string]interface{} {
	var records []map[string]interface{}
	for t, v := range e.Amounts {
		records = append(records, map[string]interface{}{
			"unique_key":           e.UniqueKey + ":" + t,
			"internal_instance_id": e.InternalInstanceID,
			"amount_type":          t,
			"amount_value":         v,
			"occurred_at":          e.OccurredAt,
			"metadata":             e.Metadata,
		})
	}
	return records
}

const (
	// ContainerSeconds is one of the billable metrics
	ContainerSeconds string = "container-seconds"
)

// Amounts is a map of amount billable metrics to their values
type Amounts map[string]int64

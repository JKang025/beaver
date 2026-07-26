package metriccollector

import "time"

type observationMetadata struct {
	key        seriesKey
	observedAt time.Time
	receivedAt time.Time
}

type observation interface {
	observationMetadata() observationMetadata
}

type countObservation struct {
	metadata observationMetadata
	value    int64
}

type recordObservation struct {
	metadata observationMetadata
	value    float64
}

func (o countObservation) observationMetadata() observationMetadata {
	return o.metadata
}

func (o recordObservation) observationMetadata() observationMetadata {
	return o.metadata
}

var (
	_ observation = countObservation{}
	_ observation = recordObservation{}
)

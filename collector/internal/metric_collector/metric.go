package metriccollector

import "time"

type ObservationMetadata struct {
	SeriesKey  SeriesKey
	ObservedAt time.Time
	ReceivedAt time.Time
}

type observation interface {
	metadata() ObservationMetadata
}

type CountObservation struct {
	Metadata ObservationMetadata
	Value    int64
}

type RecordObservation struct {
	Metadata ObservationMetadata
	Value    float64
}

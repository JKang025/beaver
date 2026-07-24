package metriccollector

import "time"

type SeriesKey struct {
	Entity string
	Series string
}

type ObservationMetadata struct {
	SeriesKey   SeriesKey
	ObservedAt  time.Time
	ReceivedAt  time.Time
	BucketStart time.Time
}

type CountObservation struct {
	Metadata ObservationMetadata
	Value    int64
}

type RecordObservation struct {
	Metadata ObservationMetadata
	Value    float64
}

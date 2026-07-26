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

func (o CountObservation) metadata() ObservationMetadata {
	return o.Metadata
}

func (o RecordObservation) metadata() ObservationMetadata {
	return o.Metadata
}

var (
	_ observation = CountObservation{}
	_ observation = RecordObservation{}
)

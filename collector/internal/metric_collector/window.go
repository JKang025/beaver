package metriccollector

import "time"

type BucketState uint8

const (
	BucketOpen BucketState = iota
	BucketClosing
	BucketSealed
)

type BucketMetadata struct {
	Start          time.Time
	State          BucketState
	PendingUpdates uint64
}

type CounterBucket struct {
	Metadata BucketMetadata
	Sum      int64
}

type GaugeBucket struct {
	Metadata BucketMetadata
	HasValue bool
	Min      float64
	Max      float64
	Last     float64
	LastAt   time.Time
	Sum      float64
	Count    uint64
}

type SampleBucket struct {
	Metadata BucketMetadata
}

type WindowMetadata struct {
	Duration       time.Duration // rolling window size
	BucketDuration time.Duration // rolling window step-size
	CurrentStart   time.Time
}

type sealedBucket interface {
	metadata() BucketMetadata
}

type seriesWindow interface {
	pushBucket(bucket sealedBucket) error
	metadata() WindowMetadata
}

type counterWindow struct {
	Metadata WindowMetadata
	buckets  []CounterBucket
}

type gaugeWindow struct {
	Metadata WindowMetadata
	buckets  []GaugeBucket
}

type sampleWindow struct {
	Metadata WindowMetadata
	buckets  []SampleBucket
}

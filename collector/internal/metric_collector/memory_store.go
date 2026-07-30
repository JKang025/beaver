package metriccollector

import "sync"

// memoryStore owns in-memory datapoints grouped by registered series.
type memoryStore struct {
	mutex      sync.RWMutex
	dataPoints map[seriesKey][]dataPoint
}

// newMemoryStore creates an empty in-memory datapoint store.
func newMemoryStore() *memoryStore {
	return &memoryStore{
		dataPoints: make(map[seriesKey][]dataPoint),
	}
}

// push accepts a completed datapoint for storage.
func (s *memoryStore) push(key seriesKey, point dataPoint) {
	// TODO: Store the datapoint and enforce retention.
}

package metriccollector

import (
	"sync"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

type seriesRegistry struct {
	mutex           sync.RWMutex
	workersBySeries map[seriesKey]*statisticsWorker
}

// seriesKey is comparable, unlike the generated protobuf MetricRef, so it can be
// used as a map key.
type seriesKey struct {
	entity string
	series string
}

func newSeriesRegistry() *seriesRegistry {
	return &seriesRegistry{
		workersBySeries: make(map[seriesKey]*statisticsWorker),
	}
}

func (r *seriesRegistry) register(
	key seriesKey,
	definition *collectorpb.Series,
	worker *statisticsWorker,
) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.workersBySeries[key]; exists {
		return false
	}

	worker.registerSeries(key, definition)
	r.workersBySeries[key] = worker
	return true
}

func (r *seriesRegistry) lookup(
	key seriesKey,
) (*statisticsWorker, *collectorpb.Series, bool) {
	r.mutex.RLock()
	worker, exists := r.workersBySeries[key]
	r.mutex.RUnlock()
	if !exists {
		return nil, nil, false
	}

	definition, exists := worker.lookupSeries(key)
	if !exists {
		return nil, nil, false
	}

	return worker, definition, true
}

func convertMetricRefToSeriesKey(ref *collectorpb.MetricRef) seriesKey {
	return seriesKey{
		entity: ref.GetEntity(),
		series: ref.GetSeries(),
	}
}

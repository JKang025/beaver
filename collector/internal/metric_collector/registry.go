package metriccollector

import (
	"sync"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

// registeredSeries associates a series definition with its assigned worker.
type registeredSeries struct {
	definition *collectorpb.Series
	worker     *statisticsWorker
}

// seriesRegistry indexes registered series for validation and worker routing.
type seriesRegistry struct {
	mutex  sync.RWMutex
	series map[seriesKey]registeredSeries
}

// seriesKey is comparable, unlike the generated protobuf MetricRef, so it can be
// used as a map key.
type seriesKey struct {
	entity string
	series string
}

func newSeriesRegistry() *seriesRegistry {
	return &seriesRegistry{
		series: make(map[seriesKey]registeredSeries),
	}
}

// register associates a new series with its definition and assigned worker.
func (r *seriesRegistry) register(
	key seriesKey,
	definition *collectorpb.Series,
	worker *statisticsWorker,
) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.series[key]; exists {
		return false
	}

	r.series[key] = registeredSeries{
		definition: definition,
		worker:     worker,
	}
	return true
}

// lookup returns a series's assigned worker and registered definition.
func (r *seriesRegistry) lookup(
	key seriesKey,
) (*statisticsWorker, *collectorpb.Series, bool) {
	r.mutex.RLock()
	registered, exists := r.series[key]
	r.mutex.RUnlock()
	if !exists {
		return nil, nil, false
	}

	return registered.worker, registered.definition, true
}

// convert protobuf type to SeriesKey
func convertMetricRefToSeriesKey(ref *collectorpb.MetricRef) seriesKey {
	return seriesKey{
		entity: ref.GetEntity(),
		series: ref.GetSeries(),
	}
}

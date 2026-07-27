package metriccollector

import (
	"context"
	"sync"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

type seriesRegistry struct {
	mutex           sync.RWMutex
	statsWorker     *statisticsWorker
	workersBySeries map[seriesKey]*statisticsWorker
}

// seriesKey is comparable, unlike the generated protobuf MetricRef, so it can be
// used as a map key.
type seriesKey struct {
	entity string
	series string
}

func newSeriesRegistry(observationBufferCapacity int) *seriesRegistry {
	return &seriesRegistry{
		statsWorker:     newStatisticsWorker(observationBufferCapacity),
		workersBySeries: make(map[seriesKey]*statisticsWorker),
	}
}

// register a series to both the registry map and statsworker internals
func (r *seriesRegistry) register(
	key seriesKey,
	definition *collectorpb.Series,
) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.workersBySeries[key]; exists {
		return false, nil
	}

	err := r.statsWorker.registerSeries(key, definition)
	if err != nil {
		return false, err
	}

	r.workersBySeries[key] = r.statsWorker
	return true, nil
}

// lookup whether series is registered
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

// convert protobuf type to SeriesKey
func convertMetricRefToSeriesKey(ref *collectorpb.MetricRef) seriesKey {
	return seriesKey{
		entity: ref.GetEntity(),
		series: ref.GetSeries(),
	}
}

// start all workers in the registry
func (r *seriesRegistry) startWorkers(ctx context.Context) {
	go r.statsWorker.run(ctx)
}

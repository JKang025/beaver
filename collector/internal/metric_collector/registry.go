package metriccollector

import (
	"sync"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type seriesRegistry struct {
	mutex           sync.RWMutex
	workersBySeries map[SeriesKey]*statisticsWorker
}

// SeriesKey is comparable, unlike the generated protobuf MetricRef, so it can be
// used as a map key.
type SeriesKey struct {
	Entity string
	Series string
}

func newSeriesRegistry() *seriesRegistry {
	return &seriesRegistry{
		workersBySeries: make(map[SeriesKey]*statisticsWorker),
	}
}

func (r *seriesRegistry) register(
	seriesKey SeriesKey,
	definition *collectorpb.Series,
	worker *statisticsWorker,
) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.workersBySeries[seriesKey]; exists {
		return false
	}

	worker.registerSeries(seriesKey, definition)
	r.workersBySeries[seriesKey] = worker
	return true
}

func (r *seriesRegistry) lookup(
	seriesKey SeriesKey,
) (*statisticsWorker, *collectorpb.Series, bool) {
	r.mutex.RLock()
	worker, exists := r.workersBySeries[seriesKey]
	r.mutex.RUnlock()
	if !exists {
		return nil, nil, false
	}

	definition, exists := worker.lookupSeries(seriesKey)
	if !exists {
		return nil, nil, false
	}

	return worker, definition, true
}

func convertMetricRefToSeriesKey(ref *collectorpb.MetricRef) SeriesKey {
	return SeriesKey{
		Entity: ref.GetEntity(),
		Series: ref.GetSeries(),
	}
}

func (s *metricsServer) lookupSeries(
	seriesKey SeriesKey,
) (*collectorpb.Series, error) {
	_, definition, exists := s.registry.lookup(seriesKey)
	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"series %q for entity %q is not registered",
			seriesKey.Series,
			seriesKey.Entity,
		)
	}

	return definition, nil
}

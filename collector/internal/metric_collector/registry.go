package metriccollector

import (
	"time"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WindowMetadata struct {
	Duration     time.Duration
	Step         time.Duration
	CurrentStart time.Time
}

type rollingWindow struct {
	metadata     WindowMetadata
	observations []observation
}

// each statisticsWorker gets a stream of observation, where it then keeps series specific info including rolling window
type statisticsWorker struct {
	observations <-chan observation
	series       map[SeriesKey]*seriesState
}

// require this pure struct to act as a key, which is why we don't use collectorpb.MetricRef
type SeriesKey struct {
	Entity string
	Series string
}

type seriesState struct {
	definition *collectorpb.Series
	window     *rollingWindow
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
	s.metricsMutex.RLock()
	defer s.metricsMutex.RUnlock()

	entityName := seriesKey.Entity
	seriesName := seriesKey.Series
	seriesMap, entityExists := s.metrics[entityName]

	if !entityExists {
		return nil, status.Errorf(codes.NotFound, "Entity %q is not registered", entityName)
	}

	series, seriesExists := seriesMap[seriesName]

	if !seriesExists {
		return nil, status.Errorf(codes.NotFound, "Series %q is not registered", seriesName)
	}

	return series, nil

}

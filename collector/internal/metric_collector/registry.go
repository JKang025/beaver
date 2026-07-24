package metriccollector

import (
	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registeredSeries struct {
	definition *collectorpb.Series
	window     seriesWindow
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

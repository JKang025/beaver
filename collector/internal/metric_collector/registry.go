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

func (s *metricsServer) lookupSeries(
	ref *collectorpb.MetricRef,
) (*collectorpb.Series, error) {
	s.metricsMutex.RLock()
	defer s.metricsMutex.RUnlock()

	entityName := ref.Entity
	seriesName := ref.Series
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

package metriccollector

import (
	"context"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type metricsServer struct {
	collectorpb.UnimplementedMetricsCollectorServer
	registry    *seriesRegistry
	statsWorker *statisticsWorker
}

func NewMetricsServer() collectorpb.MetricsCollectorServer {
	return &metricsServer{
		registry:    newSeriesRegistry(),
		statsWorker: newStatisticsWorker(),
	}
}

// CountMetric adds an integer value to a counter series.
func (s *metricsServer) CountMetric(
	ctx context.Context,
	request *collectorpb.CountMetricRequest,
) (*collectorpb.CountMetricResponse, error) {
	seriesKey := convertMetricRefToSeriesKey(request.GetMetric())
	series, err := s.lookupSeries(seriesKey)
	if err != nil {
		return nil, err
	}
	if series.GetCounter() == nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"series %q is not a counter",
			request.GetMetric().GetSeries(),
		)
	}

	// TODO: Add request.GetValue() to the counter accumulator.
	return &collectorpb.CountMetricResponse{}, nil
}

// RecordMetric records a floating-point observation in a gauge or sample series.
func (s *metricsServer) RecordMetric(
	ctx context.Context,
	request *collectorpb.RecordMetricRequest,
) (*collectorpb.RecordMetricResponse, error) {
	seriesKey := convertMetricRefToSeriesKey(request.GetMetric())
	series, err := s.lookupSeries(seriesKey)
	if err != nil {
		return nil, err
	}
	if series.GetCounter() != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"series %q is a counter; use CountMetric",
			request.GetMetric().GetSeries(),
		)
	}

	// TODO: Record request.GetValue() in the gauge or sample accumulator.
	return &collectorpb.RecordMetricResponse{}, nil
}

// RegisterMetrics registers metric series for an entity.
// Existing series and later duplicates in the request are ignored.
func (s *metricsServer) RegisterMetrics(
	ctx context.Context,
	request *collectorpb.RegisterMetricsRequest,
) (*collectorpb.RegisterMetricsResponse, error) {
	entity := request.GetEntity()
	seriesDefinitions := request.GetSeries()

	// validating basic seriesDefinition contract
	for _, seriesDefinition := range seriesDefinitions {
		seriesName := seriesDefinition.GetName()
		if seriesName == "" {
			return nil, status.Error(codes.InvalidArgument, "series name is required")
		}
		if seriesDefinition.GetConfig() == nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"series %q must have a config",
				seriesName,
			)
		}
	}

	for _, seriesDefinition := range seriesDefinitions {
		seriesKey := SeriesKey{
			Entity: entity,
			Series: seriesDefinition.GetName(),
		}
		s.registry.register(seriesKey, seriesDefinition, s.statsWorker)
	}

	return &collectorpb.RegisterMetricsResponse{}, nil
}

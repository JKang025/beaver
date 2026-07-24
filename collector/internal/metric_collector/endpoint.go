package metriccollector

import (
	"context"
	"sync"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type metricsServer struct {
	collectorpb.UnimplementedMetricsCollectorServer
	metricsMutex sync.RWMutex
	metrics      map[string]map[string]*collectorpb.Series
}

func NewMetricsServer() collectorpb.MetricsCollectorServer {
	return &metricsServer{
		metrics: make(map[string]map[string]*collectorpb.Series),
	}
}

// CountMetric adds an integer value to a counter series.
func (s *metricsServer) CountMetric(
	ctx context.Context,
	request *collectorpb.CountMetricRequest,
) (*collectorpb.CountMetricResponse, error) {
	series, err := s.lookupSeries(request.GetMetric())
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
	series, err := s.lookupSeries(request.GetMetric())
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
	if entity == "" {
		return nil, status.Error(codes.InvalidArgument, "entity is required")
	}

	seriesDefinitions := request.GetSeries()
	if len(seriesDefinitions) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one series is required")
	}

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

	s.metricsMutex.Lock()
	defer s.metricsMutex.Unlock()

	registeredSeries, entityExists := s.metrics[entity]

	if !entityExists {
		registeredSeries = make(map[string]*collectorpb.Series)
		s.metrics[entity] = registeredSeries
	}

	for _, seriesDefinition := range seriesDefinitions {
		seriesName := seriesDefinition.GetName()
		_, seriesExists := registeredSeries[seriesName]
		if seriesExists {
			continue
		}

		registeredSeries[seriesName] = seriesDefinition
	}

	return &collectorpb.RegisterMetricsResponse{}, nil
}

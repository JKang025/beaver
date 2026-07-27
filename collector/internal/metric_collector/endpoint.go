package metriccollector

import (
	"context"
	"fmt"
	"time"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type metricsServer struct {
	collectorpb.UnimplementedMetricsCollectorServer
	registry *seriesRegistry
}

const defaultObservationBufferCapacity = 1000

func NewMetricsServer(ctx context.Context) collectorpb.MetricsCollectorServer {
	registry := newSeriesRegistry(defaultObservationBufferCapacity)

	registry.startWorkers(ctx)
	return &metricsServer{
		registry: registry,
	}
}

// CountMetric adds an integer value to a counter series.
func (s *metricsServer) CountMetric(
	ctx context.Context,
	request *collectorpb.CountMetricRequest,
) (*collectorpb.CountMetricResponse, error) {
	key := convertMetricRefToSeriesKey(request.GetMetric())

	statsWorker, _, exists := s.registry.lookup(key)
	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"series %q for entity %q is not registered",
			key.series,
			key.entity,
		)
	}

	now := time.Now()
	observation := countObservation{
		metadata: observationMetadata{
			key:        key,
			observedAt: now,
			receivedAt: now, // TODO: edit RPC call to include true client side observedAt
		},
		value: request.GetValue(),
	}

	select {
	case statsWorker.observations <- observation:
		return &collectorpb.CountMetricResponse{}, nil

	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// RecordMetric records a floating-point observation in a gauge or sample series.
func (s *metricsServer) RecordMetric(
	ctx context.Context,
	request *collectorpb.RecordMetricRequest,
) (*collectorpb.RecordMetricResponse, error) {

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
		key := seriesKey{
			entity: entity,
			series: seriesDefinition.GetName(),
		}
		_, err := s.registry.register(key, seriesDefinition)
		if err != nil {
			fmt.Printf("failed to register series %s: %v\n", key.series, err)
		}
	}

	return &collectorpb.RegisterMetricsResponse{}, nil
}

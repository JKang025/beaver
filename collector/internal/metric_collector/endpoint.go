package metriccollector

import (
	"context"
	"fmt"
	"sync"
	"time"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// metricsServer validates metric RPCs and routes observations to workers.
type metricsServer struct {
	collectorpb.UnimplementedMetricsCollectorServer

	registrationMutex sync.Mutex
	registry          *seriesRegistry
	workerManager     *statisticsWorkerManager
}

const defaultObservationBufferCapacity = 1000

func NewMetricsServer(ctx context.Context) collectorpb.MetricsCollectorServer {
	registry := newSeriesRegistry()
	workerManager := newStatisticsWorkerManager(defaultObservationBufferCapacity)

	workerManager.start(ctx)
	return &metricsServer{
		registry:      registry,
		workerManager: workerManager,
	}
}

// CountMetric adds an integer value to a counter series.
func (s *metricsServer) CountMetric(
	ctx context.Context,
	request *collectorpb.CountMetricRequest,
) (*collectorpb.CountMetricResponse, error) {
	key := convertMetricRefToSeriesKey(request.GetMetric())

	statsWorker, definition, exists := s.registry.lookup(key)
	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"series %q for entity %q is not registered",
			key.series,
			key.entity,
		)
	}
	if definition.GetCounter() == nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"series %q for entity %q is not configured as a counter",
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
	key := convertMetricRefToSeriesKey(request.GetMetric())

	statsWorker, definition, exists := s.registry.lookup(key)
	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"series %q for entity %q is not registered",
			key.series,
			key.entity,
		)
	}
	if definition.GetGauge() == nil && definition.GetSample() == nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"series %q for entity %q does not accept recorded values",
			key.series,
			key.entity,
		)
	}

	now := time.Now()
	observation := recordObservation{
		metadata: observationMetadata{
			key:        key,
			observedAt: now,
			receivedAt: now,
		},
		value: request.GetValue(),
	}

	select {
	case statsWorker.observations <- observation:
		return &collectorpb.RecordMetricResponse{}, nil

	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// RegisterMetrics registers metric series for an entity.
// Existing series and later duplicates in the request are ignored.
func (s *metricsServer) RegisterMetrics(
	ctx context.Context,
	request *collectorpb.RegisterMetricsRequest,
) (*collectorpb.RegisterMetricsResponse, error) {
	entity := request.GetEntity()
	seriesDefinitions := request.GetSeries()

	if entity == "" {
		return nil, status.Error(codes.InvalidArgument, "entity is required")
	}
	if len(seriesDefinitions) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one series is required")
	}

	// Validate the complete request before registering any series.
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
		if err := s.registerSeries(key, seriesDefinition); err != nil {
			fmt.Printf("failed to register series %s: %v\n", key.series, err)
		}
	}

	return &collectorpb.RegisterMetricsResponse{}, nil
}

// registerSeries atomically initializes and publishes a series registration.
func (s *metricsServer) registerSeries(
	key seriesKey,
	definition *collectorpb.Series,
) error {
	s.registrationMutex.Lock()
	defer s.registrationMutex.Unlock()

	if _, _, exists := s.registry.lookup(key); exists {
		return nil
	}

	worker, err := s.workerManager.assignSeries(key, definition)
	if err != nil {
		return err
	}

	s.registry.register(key, definition, worker)
	return nil
}

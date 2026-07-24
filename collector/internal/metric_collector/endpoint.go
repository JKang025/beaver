package metriccollector

import (
	"context"
	"log"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

type metricsServer struct {
	collectorpb.UnimplementedMetricsCollectorServer
	metrics map[string]map[string]*metric
}

func NewMetricsServer() collectorpb.MetricsCollectorServer {
	return &metricsServer{
		metrics: make(map[string]map[string]*metric),
	}
}

func (s *metricsServer) RecordMetric(
	ctx context.Context,
	request *collectorpb.RecordMetricRequest,
) (*collectorpb.RecordMetricResponse, error) {
	log.Printf(
		"received metric: entity=%q name=%q value=%v",
		request.GetMetric().GetEntity(),
		request.GetMetric().GetName(),
		request.GetValue(),
	)

	return &collectorpb.RecordMetricResponse{}, nil
}

func (s *metricsServer) RegisterMetric(
	ctx context.Context,
	request *collectorpb.RegisterMetricRequest,
) (*collectorpb.RegisterMetricResponse, error) {
	return &collectorpb.RegisterMetricResponse{}, nil
}

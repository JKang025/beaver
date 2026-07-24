package server

import (
	metriccollector "github.com/JKang025/beaver/internal/metriccollector"
	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc"
)

// New creates the shared gRPC server and registers each application service.
func New() *grpc.Server {
	grpcServer := grpc.NewServer()

	collectorpb.RegisterMetricsCollectorServer(grpcServer, metriccollector.NewMetricsServer())

	return grpcServer
}

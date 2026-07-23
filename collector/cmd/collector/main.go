package main

import (
	"log"
	"net"

	metriccollector "github.com/JKang025/beaver/internal/metric_collector"
	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	collectorpb.RegisterMetricsCollectorServer(grpcServer, metriccollector.NewMetricsServer())

	log.Println("collector listening on :50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

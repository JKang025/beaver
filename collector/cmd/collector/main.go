package main

import (
	"log"
	"net"

	"github.com/JKang025/beaver/internal/server"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := server.New()

	log.Println("collector listening on :50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

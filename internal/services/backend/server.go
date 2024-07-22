package backend

import (
	pb "duckdb-server/internal/services/backend/data_transform"
	"fmt"
	"log"
	"net"

	grpc "google.golang.org/grpc"
)

func InitServer(host string, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDataTransformServer(grpcServer, NewDataTransformService())
	log.Printf("starting backend.data_transform server on %s:%d", host, port)
	grpcServer.Serve(lis)
}

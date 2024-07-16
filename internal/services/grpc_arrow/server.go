package grpc_arrow

import (
	pb "duckdb-server/internal/services/grpc_arrow/data_transform"
	"fmt"
	"log"
	"net"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// func InitServer(host string, port int) {
// 	var opts []grpc.ServerOption
// 	grpcServer := grpc.NewServer(opts...)
// 	pb.RegisterDataTransformServer(grpcServer, NewDataTransformService())

// 	r := gin.Default()
// 	r.GET("/transform", func(ctx *gin.Context) {
// 		grpcServer.ServeHTTP(ctx.Writer, ctx.Request)
// 	})

// 	log.Printf("strings grpc server on %s:%d", host, port)
// 	r.Run(fmt.Sprintf("%s:%d", host, port))
// }

func InitServer(host string, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDataTransformServer(grpcServer, NewDataTransformService())
	reflection.Register(grpcServer) // for grpc-curl
	log.Printf("strings grpc_arrow server on %s:%d", host, port)
	grpcServer.Serve(lis)
}

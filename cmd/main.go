package main

import (
	"duckdb-server/config"
	grpcArrow "duckdb-server/internal/services/grpc_arrow"
	"log"
	"sync"

	"github.com/joho/godotenv"
)

func main() {
	{
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error loading .env file")
		}
		config.GetConfig()
	}

	// starting gRPC server
	{
		// const (
		// 	host = "localhost"
		// 	port = 9005
		// )

		// go grpc.InitServer(host, port)
	}

	// starting gRPC server for arrow
	{
		var (
			host = config.HOST
			port = config.PORT
		)

		go grpcArrow.InitServer(host, port)
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	wg.Wait()
}

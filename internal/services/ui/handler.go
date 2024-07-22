package ui

import (
	"context"
	"duckdb-server/config"
	querybuilder "duckdb-server/internal/query_builder"
	pb "duckdb-server/internal/services/ui/data_transform"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"duckdb-server/internal/utils/crypto"
	utilsQuery "duckdb-server/internal/utils/query"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion8

// UnimplementedDataTransformServer must be embedded to have forward compatible implementations.
type dataTransform struct {
	pb.UnimplementedDataTransformServer
	// TODO: add logger
}

func NewDataTransformService() *dataTransform {
	return &dataTransform{}
}

func (t dataTransform) LoadAndQueryArrow(in *pb.LoadAndQueryArrowRequest, out pb.DataTransform_LoadAndQueryArrowServer) error {
	s := time.Now()
	defer func() {
		log.Printf("LoadAndQueryArrow completed in %f seconds\n", time.Since(s).Seconds())
	}()

	// Create sessionId and query builder
	sessionId := crypto.GetHash(fmt.Sprintf("web-%d", time.Now().Unix()))
	p := path.Join(config.DUCKDB_DIR, fmt.Sprintf("%s.duckdb", sessionId))
	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Printf("LoadAndQueryArrow: Error creating query builder, err: %v\n", err)
		return status.Errorf(codes.Internal, fmt.Errorf("could not initialize query builder, %w", err).Error())
	}

	defer func() {
		if err := qb.Close(); err != nil {
			log.Printf("LoadAndQueryArrow: Error closing the duckdb connection, err: %v\n", err)
		}

		// TODO: remove wal files
		if err := os.Remove(p); err != nil {
			log.Printf("LoadAndQueryArrow: Error removing the file, err: %v\n", err)
		}
	}()

	// Downloading file and loading data to DuckDB
	{
		downloadPath := path.Join(config.TEMP_DOWNLOAD_DIR, fmt.Sprintf("%d.parquet", time.Now().Unix()))
		f, err := os.Create(downloadPath)
		if err != nil {
			log.Printf("LoadAndQueryArrow: error creating file, err: %v\n", err)
			return err
		}

		// TODO: add validation to check file type
		if err := utilsQuery.DownloadFile(in.FilePath, f); err != nil {
			log.Printf("LoadAndQueryArrow: error downloading file, err: %v\n", err)
			return err
		}

		defer func() {
			if err := os.Remove(downloadPath); err != nil {
				log.Printf("LoadAndQueryArrow: Error removing the downloaded parquet file, err: %v\n", err)
			}
		}()

		log.Println("LoadAndQueryArrow: Loading data to duckDB")
		if err := qb.ParquetToTable(in.TableName, downloadPath); err != nil {
			log.Printf("LoadAndQueryArrow: Error loading data to duck-db, err: %v\n", err)
			return status.Errorf(codes.Internal, fmt.Errorf("could not load the file to duckdb, %w", err).Error())
		}
	}

	// Querying and streaming query result in ipc arrow format
	{
		ctx := context.Background()
		arrowQB, err := qb.GetArrow(ctx)
		if err != nil {
			log.Printf("LoadAndQueryArrow: Error creating arrow query builder, err: %v\n", err)
			return status.Errorf(codes.Internal, fmt.Errorf("could not initialize arrow query builder, %w", err).Error())
		}

		defer func() {
			if err := arrowQB.Close(); err != nil {
				log.Printf("LoadAndQueryArrow: Error closing arrow query builder, err: %v\n", err)
			}
		}()

		log.Printf("LoadAndQueryArrow: Executing the query '%s'\n", in.Query)
		rows, err := arrowQB.Query(ctx, in.Query)
		if err != nil {
			log.Printf("LoadAndQueryArrow: Error querying data, err: %s\n", err.Error())
			return status.Errorf(codes.Internal, fmt.Errorf("could not execute the query, %w", err).Error())
		}

		limit := config.CHUNK_SIZE
		sequencyNumber := 1
		log.Printf("LoadAndQueryArrow: Chunking the query result into chunks of size %d\n", limit)
		for {
			q, err := utilsQuery.GetChunkV3(rows, int64(config.CHUNK_SIZE))
			if err != nil {
				log.Printf("LoadAndQueryArrow: Error getting data, err: %v\n", err)
				return status.Errorf(codes.Internal, fmt.Errorf("could not chunk the response, %w", err).Error())
			}

			q.SequencyNumber = int32(sequencyNumber)
			if err := out.Send(q); err != nil {
				log.Printf("LoadAndQueryArrow: Error streaming data, err: %v\n", err)
				return status.Errorf(codes.Internal, fmt.Errorf("could not stream the response, %w", err).Error())
			}

			if int(q.Count) < limit {
				log.Printf("LoadAndQueryArrow: Breaking because got %d rows out of %d", q.Count, limit)
				break
			}

			sequencyNumber += 1
		}

		log.Println("LoadAndQueryArrow: Sent all chunks from the server")
	}

	return nil
}

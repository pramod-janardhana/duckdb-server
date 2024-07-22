package backend

import (
	"context"
	"duckdb-server/config"
	querybuilder "duckdb-server/internal/query_builder"
	pb "duckdb-server/internal/services/backend/data_transform"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"duckdb-server/internal/utils/crypto"
	utilsFile "duckdb-server/internal/utils/file"
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

func (t dataTransform) Load(ctx context.Context, in *pb.LoadRequests) (*pb.LoadResponse, error) {
	s := time.Now()
	defer func() {
		log.Printf("LocalLoad completed in %f seconds\n", time.Since(s).Seconds())
	}()

	// Create sessionId and query builder
	sessionId := crypto.GetHash(fmt.Sprintf("%s-%d", in.JobId, time.Now().Unix()))
	p := path.Join(config.DUCKDB_DIR, fmt.Sprintf("%s.duckdb", sessionId))
	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Printf("Load: Error creating query builder, err: %v\n", err)
		return nil, status.Errorf(codes.Internal, fmt.Errorf("could not initialize query builder, %w", err).Error())
	}

	defer func() { qb.Close() }()

	log.Println("Load: Loading data to duckDB")
	if err := qb.CSVToTable(in.TableName, in.FilePath); err != nil {
		log.Printf("Load: Error loading data to duck-db, err: %v\n", err)
		return nil, status.Errorf(codes.Internal, fmt.Errorf("could not load the file to duckdb, %w", err).Error())
	}

	return &pb.LoadResponse{SessionId: sessionId}, nil
}

func (t dataTransform) WriteToParquet(ctx context.Context, in *pb.WriteToParquetRequests) (*pb.WriteToParquetResponse, error) {
	s := time.Now()
	defer func() {
		log.Printf("WriteToParquet completed in %f seconds\n", time.Since(s).Seconds())
	}()

	// Create sessionId and query builder
	p := path.Join(config.DUCKDB_DIR, fmt.Sprintf("%s.duckdb", in.SessionId))
	exists, err := utilsFile.PathExists(p)
	if err != nil {
		log.Printf("WriteToParquet: Error checking if path exist, err: %v\n", err)
		return nil, status.Errorf(codes.Internal, fmt.Errorf("could not validate the session, %w", err).Error())
	}

	if !exists {
		log.Printf("WriteToParquet: Invalid session\n")
		return nil, status.Errorf(codes.Unavailable, fmt.Errorf("invalid session").Error())
	}

	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Printf("WriteToParquet: Error creating query builder, err: %v\n", err)
		return nil, status.Errorf(codes.Internal, fmt.Errorf("could not initialize query builder, %w", err).Error())
	}

	defer func() { qb.Close() }()

	log.Printf("WriteToParquet: Exporting result as parquet to '%s'\n", in.FileExportPath)
	_, err = qb.Query(fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION 'zstd');", in.Query, in.FileExportPath))
	if err != nil {
		log.Printf("WriteToParquet: Error writing data to parquet, err: %s\n", err.Error())
		return nil, status.Errorf(codes.Internal, fmt.Errorf("could not export result as parquet, %w", err).Error())
	}

	return &pb.WriteToParquetResponse{}, nil
}

func (t dataTransform) QueryArrow(in *pb.QueryArrowRequests, out pb.DataTransform_QueryArrowServer) error {
	s := time.Now()
	defer func() {
		log.Printf("QueryArrow completed in %f seconds\n", time.Since(s).Seconds())
	}()

	// Create sessionId and query builder
	p := path.Join(config.DUCKDB_DIR, fmt.Sprintf("%s.duckdb", in.SessionId))
	exists, err := utilsFile.PathExists(p)
	if err != nil {
		log.Printf("WriteToParquet: Error checking if path exist, err: %v\n", err)
		return status.Errorf(codes.Internal, fmt.Errorf("could not validate the session, %w", err).Error())
	}

	if !exists {
		log.Printf("WriteToParquet: Invalid session\n")
		return status.Errorf(codes.Unavailable, fmt.Errorf("invalid session").Error())
	}

	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Printf("QueryArrow: Error creating query builder, err: %v\n", err)
		return status.Errorf(codes.Internal, fmt.Errorf("could not initialize query builder, %w", err).Error())
	}

	defer func() { qb.Close() }()

	ctx := context.Background()
	arrowQB, err := qb.GetArrow(ctx)
	if err != nil {
		log.Printf("QueryArrow: Error creating arrow query builder, err: %v\n", err)
		return status.Errorf(codes.Internal, fmt.Errorf("could not initialize arrow query builder, %w", err).Error())
	}

	defer func() {
		if err := arrowQB.Close(); err != nil {
			log.Printf("QueryArrow: Error closing arrow query builder, err: %v\n", err)
		}
	}()

	log.Printf("QueryArrow: Executing the query '%s'\n", in.Query)
	rows, err := arrowQB.Query(ctx, in.Query)
	if err != nil {
		log.Printf("QueryArrow: Error querying data, err: %s\n", err.Error())
		return status.Errorf(codes.Internal, fmt.Errorf("could not execute the query, %w", err).Error())
	}

	limit := config.CHUNK_SIZE
	sequencyNumber := 1
	log.Printf("QueryArrow: Chunking the query result into chunks of size %d\n", limit)
	for {
		q, err := utilsQuery.GetChunkV2(rows, int64(config.CHUNK_SIZE))
		if err != nil {
			log.Printf("QueryArrow: Error getting data, err: %v\n", err)
			return status.Errorf(codes.Internal, fmt.Errorf("could not chunk the response, %w", err).Error())
		}

		q.SequencyNumber = int32(sequencyNumber)
		if err := out.Send(q); err != nil {
			log.Printf("QueryArrow: Error streaming data, err: %v\n", err)
			return status.Errorf(codes.Internal, fmt.Errorf("could not stream the response, %w", err).Error())
		}

		if int(q.Count) < limit {
			log.Printf("QueryArrow: Breaking because got %d rows out of %d", q.Count, limit)
			break
		}

		sequencyNumber += 1
	}

	log.Println("QueryArrow: Sent all chunks from the server")
	return nil
}

func (t dataTransform) Clear(ctx context.Context, in *pb.ClearRequest) (*pb.ClearResponse, error) {
	s := time.Now()
	defer func() {
		log.Printf("Clear completed in %f seconds\n", time.Since(s).Seconds())
	}()

	// Create sessionId and query builder
	p := path.Join(config.DUCKDB_DIR, fmt.Sprintf("%s.duckdb", in.SessionId))
	exists, err := utilsFile.PathExists(p)
	if err != nil {
		log.Printf("Clear: Error checking if path exist, err: %v\n", err)
		return nil, status.Errorf(codes.Internal, fmt.Errorf("could not validate the session, %w", err).Error())
	}

	if !exists {
		log.Printf("Clear: Invalid session\n")
		return nil, status.Errorf(codes.Unavailable, fmt.Errorf("invalid session").Error())
	}

	// TODO: remove wal files
	err = os.Remove(p)
	if err != nil {
		log.Fatalf("Clear: Error removing the file, err: %v\n", err)
	}

	log.Printf("Clear: Cleared the session with id '%s'\n", in.SessionId)
	return &pb.ClearResponse{}, nil
}

package grpc_arrow

import (
	"context"
	"duckdb-server/config"
	querybuilder "duckdb-server/internal/query_builder"
	pb "duckdb-server/internal/services/grpc_arrow/data_transform"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"strings"
	"time"

	utilsQuery "duckdb-server/internal/utils/query"

	grpc "google.golang.org/grpc"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// UnimplementedDataTransformServer must be embedded to have forward compatible implementations.
type dataTransform struct {
	pb.UnimplementedDataTransformServer
	qb *querybuilder.DuckDBQueryBuilder
}

func NewDataTransformService() *dataTransform {
	p := path.Join(config.DUCKDB_DIR, fmt.Sprintf("data-%d.duckdb", time.Now().Unix()))
	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Fatalf("Error creating query builder, err: %v\n", err)
	}

	return &dataTransform{
		qb: qb,
	}
}

func (t dataTransform) TransformAndStreamArrow(in *pb.QueryIn, stream pb.DataTransform_TransformAndStreamArrowServer) error {
	defer func() {
		log.Println("computed transform")
	}()

	// mem profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("online-arrow-grpc-mem-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		defer pprof.WriteHeapProfile(f)
	}

	// cpu profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("online-arrow-grpc-cpu-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()
	arrowQB, err := t.qb.GetArrow(ctx)
	if err != nil {
		log.Fatalf("Error creating arrow query builder, err: %v\n", err)
	}

	defer func() {
		if err := arrowQB.Close(); err != nil {
			log.Printf("Error closing arrow query builder, err: %v\n", err)
		}
	}()

	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	var filePath string
	if !strings.Contains(in.Path, "https://") {
		filePath = in.Path
	} else {
		log.Println("Downloading file since received path is http(s)")
		filePath = path.Join(config.TEMP_DOWNLOAD_DIR, fmt.Sprintf("%d .csv", time.Now().Unix()))
		f, err := os.Create(filePath)
		if err != nil {
			log.Printf("error creating file, err: %v\n", err)
			return err
		}

		if err := utilsQuery.DownloadFile(in.Path, f); err != nil {
			log.Printf("error downloading file, err: %v\n", err)
			return err
		}
	}

	log.Println("Loading data to duckDB")
	if err := t.qb.CSVToTable(tableName, filePath); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return err
	}

	log.Println("Creating view for the transformation query")
	if err := t.qb.Exec(utilsQuery.CreateView(viewName, tableName)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return err
	}

	log.Println("Querying the view")
	rows, err := arrowQB.Query(ctx, fmt.Sprintf("SELECT * FROM %s", viewName))
	if err != nil {
		log.Printf("Error querying data, err: %s\n", err.Error())
		return err
	}

	limit := config.CHUNK_SIZE
	sequencyNumber := 1
	log.Println("Chunking the query result")
	for {
		q, err := utilsQuery.GetChunk(rows, int64(config.CHUNK_SIZE))
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return err
		}

		q.SequencyNumber = int32(sequencyNumber)
		if err := stream.Send(q); err != nil {
			log.Printf("error streaming data, err: %v\n", err)
			return err
		}

		if int(q.Count) < limit {
			log.Printf("breaking because got %d rows out of %d", q.Count, limit)
			break
		}

		// offset += limit
		sequencyNumber += 1
	}

	log.Println("Sent all chunks from the server")
	return nil
}

func (t dataTransform) TransformAndStreamParquet(in *pb.QueryIn, stream pb.DataTransform_TransformAndStreamParquetServer) error {
	defer func() {
		log.Println("computed transform")
	}()

	// mem profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("online-parquet-grpc-mem-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		defer pprof.WriteHeapProfile(f)
	}

	// cpu profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("online-parquet-grpc-cpu-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()
	arrowQB, err := t.qb.GetArrow(ctx)
	if err != nil {
		log.Fatalf("Error creating arrow query builder, err: %v\n", err)
	}

	defer func() {
		if err := arrowQB.Close(); err != nil {
			log.Printf("Error closing arrow query builder, err: %v\n", err)
		}
	}()

	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	downloadPath := path.Join(config.TEMP_DOWNLOAD_DIR, fmt.Sprintf("%d.csv", time.Now().Unix()))
	f, err := os.Create(downloadPath)
	if err != nil {
		log.Printf("error creating file, err: %v\n", err)
		return err
	}

	if err := utilsQuery.DownloadFile(in.Path, f); err != nil {
		log.Printf("error downloading file, err: %v\n", err)
		return err
	}

	log.Println("Loading data to duckDB")
	if err := t.qb.CSVToTable(tableName, downloadPath); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return err
	}

	log.Println("Creating view for the transformation query")
	if err := t.qb.Exec(utilsQuery.CreateViewV2(viewName, in.Query)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return err
	}

	log.Println("Querying the view")
	exportPath := path.Join(config.TEMP_DOWNLOAD_DIR, fmt.Sprintf("%d.parquet", time.Now().Unix()))
	_, err = t.qb.Query(fmt.Sprintf("COPY %s TO '%s' (ENCRYPTION_CONFIG {footer_key: 'key256'}, FORMAT PARQUET, COMPRESSION 'gzip');", viewName, exportPath))
	if err != nil {
		log.Printf("Error writing data to parquet, err: %s\n", err.Error())
		return err
	}

	outFile, err := os.Open(exportPath)
	if err != nil {
		log.Printf("Error reading data from parquet, err: %s\n", err.Error())
		return err
	}

	sequencyNumber := 1
	buf := make([]byte, config.FILE_CHUNK_SIZE)
	log.Println("Chunking the query result")
	for {
		q := &pb.QueryOut{}
		_, err := outFile.Read(buf)
		if err == io.EOF {
			log.Printf("reached EOF thus breaking\n")
			break
		}
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return err
		}

		q.SequencyNumber = int32(sequencyNumber)
		if err := stream.Send(q); err != nil {
			log.Printf("error streaming data, err: %v\n", err)
			return err
		}

		sequencyNumber += 1
	}

	log.Println("Sent all chunks from the server")
	return nil
}

func (t dataTransform) LocalTransformAndStreamArrow(in *pb.QueryIn, stream pb.DataTransform_LocalTransformAndStreamArrowServer) error {
	defer func() {
		log.Println("computed transform")
	}()

	// mem profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("local-arrow-grpc-mem-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		defer pprof.WriteHeapProfile(f)
	}

	// cpu profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("local-arrow-grpc-cpu-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()
	arrowQB, err := t.qb.GetArrow(ctx)
	if err != nil {
		log.Fatalf("Error creating arrow query builder, err: %v\n", err)
	}

	defer func() {
		if err := arrowQB.Close(); err != nil {
			log.Printf("Error closing arrow query builder, err: %v\n", err)
		}
	}()

	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	log.Println("Loading data to duckDB")
	if err := t.qb.CSVToTable(tableName, in.Path); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return err
	}

	log.Println("Creating view for the transformation query")
	if err := t.qb.Exec(utilsQuery.CreateViewV2(viewName, in.Query)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return err
	}

	log.Println("Querying the view")
	rows, err := arrowQB.Query(ctx, fmt.Sprintf("SELECT * FROM %s", viewName))
	if err != nil {
		log.Printf("Error querying data, err: %s\n", err.Error())
		return err
	}

	limit := config.CHUNK_SIZE
	sequencyNumber := 1
	log.Println("Chunking the query result")
	for {
		// q, err := utilsQuery.ArrowTransformV2(arrowQB, fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", viewName, limit, offset))
		q, err := utilsQuery.GetChunk(rows, int64(config.CHUNK_SIZE))
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return err
		}

		q.SequencyNumber = int32(sequencyNumber)
		if err := stream.Send(q); err != nil {
			log.Printf("error streaming data, err: %v\n", err)
			return err
		}

		if int(q.Count) < limit {
			log.Printf("breaking because got %d rows out of %d", q.Count, limit)
			break
		}

		// offset += limit
		sequencyNumber += 1
	}

	log.Println("Sent all chunks from the server")
	return nil
}

func (t dataTransform) LocalTransformAndStreamParquet(in *pb.QueryIn, stream pb.DataTransform_LocalTransformAndStreamParquetServer) error {
	defer func() {
		log.Println("computed transform")
	}()

	// mem profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("local-parquet-grpc-mem-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		defer pprof.WriteHeapProfile(f)
	}

	// cpu profiler
	{
		p := path.Join(config.TEMP_PROF_DIR, fmt.Sprintf("local-parquet-grpc-cpu-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()
	arrowQB, err := t.qb.GetArrow(ctx)
	if err != nil {
		log.Fatalf("Error creating arrow query builder, err: %v\n", err)
	}

	defer func() {
		if err := arrowQB.Close(); err != nil {
			log.Printf("Error closing arrow query builder, err: %v\n", err)
		}
	}()

	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	log.Println("Loading data to duckDB")
	if err := t.qb.CSVToTable(tableName, in.Path); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return err
	}

	log.Println("Creating view for the transformation query")
	if err := t.qb.Exec(utilsQuery.CreateViewV2(viewName, in.Query)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return err
	}

	log.Println("Querying the view")
	exportPath := path.Join(config.TEMP_DOWNLOAD_DIR, fmt.Sprintf("%d.parquet", time.Now().Unix()))
	_, err = t.qb.Query(fmt.Sprintf("COPY %s TO '%s' (ENCRYPTION_CONFIG {footer_key: 'key256'}, FORMAT PARQUET, COMPRESSION 'gzip');", viewName, exportPath))
	if err != nil {
		log.Printf("Error writing data to parquet, err: %s\n", err.Error())
		return err
	}

	outFile, err := os.Open(exportPath)
	if err != nil {
		log.Printf("Error reading data from parquet, err: %s\n", err.Error())
		return err
	}

	sequencyNumber := 1
	buf := make([]byte, config.FILE_CHUNK_SIZE)
	log.Println("Chunking the query result")
	for {
		q := &pb.QueryOut{}
		_, err := outFile.Read(buf)
		if err == io.EOF {
			log.Printf("reached EOF thus breaking\n")
			break
		}
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return err
		}

		q.SequencyNumber = int32(sequencyNumber)
		if err := stream.Send(q); err != nil {
			log.Printf("error streaming data, err: %v\n", err)
			return err
		}

		sequencyNumber += 1
	}

	log.Println("Sent all chunks from the server")
	return nil
}

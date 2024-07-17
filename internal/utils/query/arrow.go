package query

import (
	"bytes"
	"context"
	querybuilder "duckdb-server/internal/query_builder"
	pb "duckdb-server/internal/services/grpc_arrow/data_transform"
	"log"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
)

func ArrowTransformV2(qb *querybuilder.DuckDBArrowQueryBuilder, query string) (*pb.QueryOut, error) {
	queryOut := pb.QueryOut{
		SequencyNumber: 1,
		Count:          1,
		Data:           [][]byte{},
	}

	ctx := context.Background()
	defer ctx.Done()

	rows, err := qb.Query(ctx, query)
	if err != nil {
		log.Printf("Error querying data, err: %s\n", err.Error())
		return nil, err
	}

	var count int64 = 0
	for rows.Next() {
		record := rows.Record()
		data, err := record.MarshalJSON()
		if err != nil {
			log.Printf("Error marshaling record, err: %s\n", err.Error())
		}

		queryOut.Data = append(queryOut.Data, data)
		count += record.NumRows()
		record.Release()
	}

	queryOut.Count = int32(count)
	return &queryOut, nil
}

func GetChunk(rows array.RecordReader, size int64) (*pb.QueryOut, error) {
	queryOut := pb.QueryOut{
		SequencyNumber: 1,
		Count:          1,
		Data:           [][]byte{},
	}

	var count int64 = 0
	for count < size && rows.Next() {
		record := rows.Record()

		var buf []byte
		var bufReader = bytes.NewBuffer(buf)
		writer := ipc.NewWriter(bufReader, ipc.WithSchema(record.Schema()))
		if err := writer.Write(record); err != nil {
			log.Printf("GetChunk: Error marshaling record, err: %s\n", err.Error())
		}

		queryOut.Data = append(queryOut.Data, bufReader.Bytes())
		count += record.NumRows()
		record.Release()
	}

	queryOut.Count = int32(count)
	return &queryOut, nil
}

package querybuilder

import (
	"context"
	"database/sql/driver"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/marcboeker/go-duckdb"
)

type DuckDBArrowQueryBuilder struct {
	arrow *duckdb.Arrow
	conn  driver.Conn
}

func NewDuckDBArrowQueryBuilder(ctx context.Context, sql *duckdb.Connector) (*DuckDBArrowQueryBuilder, error) {
	conn, err := sql.Connect(ctx)
	if err != nil {
		return nil, err
	}

	arrow, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		return nil, err
	}

	return &DuckDBArrowQueryBuilder{arrow: arrow, conn: conn}, nil
}

func (qb DuckDBArrowQueryBuilder) Exec(ctx context.Context, query string) (array.RecordReader, error) {
	return qb.arrow.QueryContext(ctx, query)
}

func (qb DuckDBArrowQueryBuilder) Query(ctx context.Context, query string) (array.RecordReader, error) {
	return qb.Exec(ctx, query)
}

func (qb DuckDBArrowQueryBuilder) Close() error {
	return qb.conn.Close()
}

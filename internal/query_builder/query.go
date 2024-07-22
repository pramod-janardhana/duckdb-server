package querybuilder

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/marcboeker/go-duckdb"
)

const (
	DEFAULT_PATH = "./data.duckdb"
)

type DuckDBQueryBuilder struct {
	con       *sql.DB
	connector *duckdb.Connector
}

func NewDuckDBQueryBuilder(path string) (*DuckDBQueryBuilder, error) {
	if len(path) == 0 {
		path = DEFAULT_PATH
	}

	con, err := duckdb.NewConnector(path, nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(con)

	db.Exec("PRAGMA memory_limit='2GB'")
	db.Exec("SET temp_directory='/tmp/duckdb_tmp'")
	// db.Exec("SET threads TO 4")
	// db.Exec("SET max_temp_directory_size='8GB'")
	// db.Exec("SET default_block_size=2621440")
	// db.Exec("SET enable_progress_bar = true")
	db.Exec("PRAGMA add_parquet_key('key256', '01234567891123450123456789112345');")
	log.Println("added memory_limit and temp_directory")

	return &DuckDBQueryBuilder{con: db, connector: con}, nil
}

func (qb DuckDBQueryBuilder) CSVToTable(tableName, filePath string) error {
	return qb.Exec(fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_csv('%s');`, tableName, filePath))
}

func (qb DuckDBQueryBuilder) ParquetToTable(tableName, filePath string) error {
	return qb.Exec(fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_parquet('%s');`, tableName, filePath))
}

func (qb DuckDBQueryBuilder) Exec(query string) error {
	_, err := qb.con.Exec(query)
	return err
}

func (qb DuckDBQueryBuilder) Query(query string) (*sql.Rows, error) {
	return qb.con.Query(query)
}

func (qb DuckDBQueryBuilder) Close() error {
	return qb.con.Close()
}

func (qb DuckDBQueryBuilder) GetArrow(ctx context.Context) (*DuckDBArrowQueryBuilder, error) {
	return NewDuckDBArrowQueryBuilder(ctx, qb.connector)
}

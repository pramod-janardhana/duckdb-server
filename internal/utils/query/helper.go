package query

import (
	"context"
	"database/sql"
	querybuilder "duckdb-server/internal/query_builder"
	pb "duckdb-server/internal/services/grpc/data_transform"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/apache/arrow/go/v14/arrow"
)

type ArrowQueryOut struct {
	SequencyNumber int32
	Count          int32
	Data           *Data
}

type Data struct {
	Columns []string
	Rows    []arrow.Record
}

func ArrowTransform(qb *querybuilder.DuckDBArrowQueryBuilder, query string) (*ArrowQueryOut, error) {
	queryOut := ArrowQueryOut{
		SequencyNumber: 1,
		Count:          1,
		Data: &Data{
			Columns: []string{},
			Rows:    []arrow.Record{},
		},
	}

	ctx := context.Background()
	defer ctx.Done()

	rows, err := qb.Query(ctx, query)
	if err != nil {
		log.Printf("Error querying data, err: %s\n", err.Error())
		return nil, err
	}

	// Get column names
	// rec := rows.Record()

	// for _, column := range rec.Columns() {
	// 	queryOut.Data.Columns = append(queryOut.Data.Columns, column.String())
	// }

	if rows.Next() {
		record := rows.Record()
		queryOut.Data.Rows = append(queryOut.Data.Rows, record)

		for i := 0; i < int(record.NumCols()); i++ {
			queryOut.Data.Columns = append(queryOut.Data.Columns, record.ColumnName(i))
		}

		// record.Release()
	}

	for rows.Next() {
		record := rows.Record()
		queryOut.Data.Rows = append(queryOut.Data.Rows, record)
		// record.Release()
	}

	return &queryOut, nil
}

func Transform(qb *querybuilder.DuckDBQueryBuilder, query string) (*pb.QueryOut, error) {
	queryOut := pb.QueryOut{
		SequencyNumber: 1,
		Count:          1,
		Data: &pb.Data{
			Columns: &pb.Columns{
				Name: []string{},
			},
			Rows: []*pb.Row{},
		},
	}

	rows, err := qb.Query(query)
	if err != nil {
		log.Printf("Error querying data, err: %s\n", err.Error())
		return nil, err
	}

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Printf("Error getting col details, err: %s\n", err.Error())
		return nil, err
	}

	queryOut.Data.Columns.Name = append(queryOut.Data.Columns.Name, columns...)

	// Create a slice of any's to represent each column
	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(any)
	}

	for rows.Next() {
		err = rows.Scan(values...)
		if errors.Is(err, sql.ErrNoRows) {
			log.Println("no rows")
			break
		} else if err != nil {
			log.Printf("error scanning record, err: %v\n", err)
			return nil, err
		}

		record := pb.Row{Values: make([]string, len(columns))}
		for i := 0; i < len(columns); i++ {
			record.Values[i] = fmt.Sprintf("%v", *values[i].(*interface{}))
		}

		queryOut.Data.Rows = append(queryOut.Data.Rows, &record)
	}

	return &queryOut, nil
}

func CreateView(viewName, tableName string) string {
	return fmt.Sprintf(`create or replace view %s as WITH 
    cte_2_210717_0 AS (SELECT * FROM %s), 
    cte_2_210717_1 AS (select 
        "Customer Id", "First Name", "Last Name", Company, City, 
        Country, "Phone 1", "Phone 2", Email, Website,  
          "Subscription Date", Index from cte_2_210717_0
    ), cte_2_210717_2 AS (
        select 
            CASE WHEN "Customer Id" IS NULL THEN '(blank)' ELSE  "Customer Id" END as "Customer Id", 
            CASE WHEN "First Name" IS NULL THEN '(blank)' ELSE  "First Name" END as "First Name", 
            CASE WHEN "Last Name" IS NULL THEN '(blank)' ELSE  "Last Name" END as "Last Name", 
            CASE WHEN Company IS NULL THEN '(blank)' ELSE  Company END as Company, 
            CASE WHEN City IS NULL THEN '(blank)' ELSE  City END as City, 
            CASE WHEN Country IS NULL THEN '(blank)' ELSE  Country END as Country, 
            CASE WHEN "Phone 1" IS NULL THEN '(blank)' ELSE  "Phone 1" END as "Phone 1", 
            CASE WHEN "Phone 2" IS NULL THEN '(blank)' ELSE  "Phone 2" END as "Phone 2", 
            CASE WHEN Email IS NULL THEN '(blank)' ELSE  Email END as Email, 
            Website,    "Subscription Date", Index 
        from cte_2_210717_1
    ), cte_2_210717_3 AS (
        select 
            "Customer Id", "First Name", "Last Name", Company, City, 
            Country, "Phone 1", "Phone 2", Email, Website,  
              min("Subscription Date") as "Subscription Date", sum(Index) as Index 
        from cte_2_210717_2 
        group by GROUPING SETS (
            ("Customer Id"),("Customer Id","First Name"),("Customer Id","First Name","Last Name"),
            ("Customer Id","First Name","Last Name",Company),("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ("Customer Id"),
            ("Customer Id"),("Customer Id"),
            ("Customer Id","First Name"),("Customer Id","First Name"),
            ("Customer Id","First Name"),("Customer Id","First Name","Last Name"),
            ("Customer Id","First Name","Last Name"),
            ("Customer Id","First Name","Last Name"),(
            "Customer Id","First Name","Last Name",Company),("Customer Id","First Name","Last Name",Company),
            ("Customer Id","First Name","Last Name",Company),
            ("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ()
        )
    ) (select * from cte_2_210717_3);`, viewName, tableName)
}

func CreateViewV2(viewName, query string) string {
	return fmt.Sprintf(`create or replace view %s as %s;`, viewName, query)
}

func getQuery(tableName string) string {
	return fmt.Sprintf(`select * from ( WITH 
    cte_2_210717_0 AS (SELECT * FROM %s), 
    cte_2_210717_1 AS (select 
        "Customer Id", "First Name", "Last Name", Company, City, 
        Country, "Phone 1", "Phone 2", Email, Website,  
          "Subscription Date", Index from cte_2_210717_0
    ), cte_2_210717_2 AS (
        select 
            CASE WHEN "Customer Id" IS NULL THEN '(blank)' ELSE  "Customer Id" END as "Customer Id", 
            CASE WHEN "First Name" IS NULL THEN '(blank)' ELSE  "First Name" END as "First Name", 
            CASE WHEN "Last Name" IS NULL THEN '(blank)' ELSE  "Last Name" END as "Last Name", 
            CASE WHEN Company IS NULL THEN '(blank)' ELSE  Company END as Company, 
            CASE WHEN City IS NULL THEN '(blank)' ELSE  City END as City, 
            CASE WHEN Country IS NULL THEN '(blank)' ELSE  Country END as Country, 
            CASE WHEN "Phone 1" IS NULL THEN '(blank)' ELSE  "Phone 1" END as "Phone 1", 
            CASE WHEN "Phone 2" IS NULL THEN '(blank)' ELSE  "Phone 2" END as "Phone 2", 
            CASE WHEN Email IS NULL THEN '(blank)' ELSE  Email END as Email, 
            Website,    "Subscription Date", Index 
        from cte_2_210717_1
    ), cte_2_210717_3 AS (
        select 
            "Customer Id", "First Name", "Last Name", Company, City, 
            Country, "Phone 1", "Phone 2", Email, Website,  
              min("Subscription Date") as "Subscription Date", sum(Index) as Index 
        from cte_2_210717_2 
        group by GROUPING SETS (
            ("Customer Id"),("Customer Id","First Name"),("Customer Id","First Name","Last Name"),
            ("Customer Id","First Name","Last Name",Company),("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ("Customer Id"),
            ("Customer Id"),("Customer Id"),
            ("Customer Id","First Name"),("Customer Id","First Name"),
            ("Customer Id","First Name"),("Customer Id","First Name","Last Name"),
            ("Customer Id","First Name","Last Name"),
            ("Customer Id","First Name","Last Name"),(
            "Customer Id","First Name","Last Name",Company),("Customer Id","First Name","Last Name",Company),
            ("Customer Id","First Name","Last Name",Company),
            ("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2"),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ("Customer Id","First Name","Last Name",Company,City,Country,"Phone 1","Phone 2",Email,Website),
            ()
        ) order by 
        "Customer Id" NULLS FIRST,"First Name" NULLS FIRST,"Last Name" NULLS FIRST,
        Company NULLS FIRST,City NULLS FIRST,Country NULLS FIRST,"Phone 1" NULLS FIRST,
        "Phone 2" NULLS FIRST,Email NULLS FIRST,Website NULLS FIRST,
        "Subscription Date" ASC
    ) (select * from cte_2_210717_3));`, tableName)
}

func DownloadFile(url string, out io.Writer) error {
    log.Printf("Downloading file: %s", url)
    res, err := http.Get(url)
    if err!= nil {
        return err
    }

    defer res.Body.Close()
    _, err = io.Copy(out, res.Body)
    return err
}
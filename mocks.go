package dbtmock

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
)

/*
   Represents a Mock as SQL
*/
type SQLMock struct {
	Sql     string
	Columns []string
}

/*
   Utility Function, converts a CSV file into a List of dictionaries.
   Each row is converted into a dictionary where the keys are columns.
*/
func CSVToMap(reader io.Reader) []map[string]string {

	r := csv.NewReader(reader)
	rows := []map[string]string{}
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return rows
}

// converts a csv row's single column value into a SQL statement
func mockEntryToSql(columnName string, value string, columnType string) string {

	if value == "" {
		value = "null"
	} else {
		value = fmt.Sprintf("\"%s\"", value)
	}
	if columnType != "" {
		return fmt.Sprintf("CAST(%s AS %s) AS %s", value, columnType, columnName)
	}

	return fmt.Sprintf("%s AS %s", value, columnName)

}

/*
   Converts a Mock into a SQL statement that we can use in Replacements
*/
func mockToSql(m Mock) (SQLMock, error) {

	allColumns := []string{}
	file, err := os.Open(m.Filepath)
	if err != nil {
		return SQLMock{}, err

	}
	data := CSVToMap(file)
	var sqlStatements []string
	for _, row := range data {

		columnsValues := []string{}
		columns := make([]string, 0)
		// ordering columns so we can test
		for k, _ := range row {
			columns = append(columns, k)
		}
		sort.Strings(columns)
		if len(allColumns) == 0 {
			allColumns = columns
		}
		for _, column := range columns {
			value := row[column]
			columnType := m.Types[column]
			entry := mockEntryToSql(column, value, columnType)
			columnsValues = append(columnsValues, entry)

		}
		statement := fmt.Sprintf("\n SELECT %s", strings.Join(columnsValues, ", "))
		sqlStatements = append(sqlStatements, statement)
	}
	return SQLMock{Sql: strings.Join(sqlStatements, "\n UNION ALL \n"), Columns: allColumns}, nil

}

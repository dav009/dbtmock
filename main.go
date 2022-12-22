package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

/* Describing Tests as structures */
type Mock struct {
	Name     string            `json:"name"`
	Filepath string            `json:"filepath"`
	Types    map[string]string `json:"types"`
}

type Replacement struct {
	TableFullName  string
	ReplaceSql     string
	TableShortName string
}

type Output struct {
	Name string `json:"name"`
}

type Test struct {
	Name   string          `json:"name"`
	Model  string          `json:"model"`
	Mocks  map[string]Mock `json:"mocks"`
	Output Mock            `json:"output"`
}

/* Manifest parsing */
type Source struct {
	Name         string                 `json:"name"`
	UniqueId     string                 `json:"unique_id"`
	Fqn          []string               `json:"fqn"`
	RelationName string                 `json:"relation_name"`
	X            map[string]interface{} `json:"-"`
}

type Node struct {
	Compiledcode string                 `json:"compiled_code"`
	DependsOn    map[string][]string    `json:"depends_on"`
	Alias        string                 `json:"alias"`
	Database     string                 `json:"database"`
	Schema       string                 `json:"schema"`
	Name         string                 `json:"name"`
	X            map[string]interface{} `json:"-"`
}

type Manifest struct {
	Nodes   map[string]Node        `json:"nodes"`
	Sources map[string]Source      `json:"sources"`
	X       map[string]interface{} `json:"-"`
}

// returns a Manifest structure out of a .json file
func parseManifest(path string) Manifest {

	jsonFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	bytes, _ := ioutil.ReadAll(jsonFile)
	manifest := Manifest{}
	if err := json.Unmarshal(bytes, &manifest); err != nil {
		panic(err)
	}
	return manifest
}

func isModel(nodeKey string) bool {
	return strings.HasPrefix(nodeKey, "model") || strings.HasPrefix(nodeKey, "seed")
}

/*
   Given a SQL query and a replacement Struct, it applies the recplament on the SQL and returns a new SQL query.   References to a Table are replaced
*/
func Replace(sql string, replacement Replacement) string {

	if (replacement == Replacement{}) {
		return sql
	}
	regexWithAlias, err := regexp.Compile(fmt.Sprintf(`(?i)%s\sas\s([A-z0-9_]+)\s`, replacement.TableFullName))
	if err != nil {
		panic(err)
	}
	useExistingAlias := fmt.Sprintf("(%s) AS $1 ", replacement.ReplaceSql)
	newSql := regexWithAlias.ReplaceAllString(sql, useExistingAlias)
	createAlias := fmt.Sprintf("(%s) AS %s", replacement.ReplaceSql, replacement.TableShortName)
	newSql = strings.ReplaceAll(newSql, fmt.Sprintf("%s", replacement.TableFullName), createAlias)
	return newSql
}

/*
   Returns the SQL code for a model.
   The returned SQL has replacement for the specified mocks.
*/
func sqlModel(manifest Manifest, nodeKey string, mocks map[string]Mock) (Replacement, error) {

	currentNode := manifest.Nodes[nodeKey]
	fullname := fmt.Sprintf("`%s`.`%s`.`%s`", currentNode.Database, currentNode.Schema, currentNode.Name)
	shortname := currentNode.Alias

	// if in mocks return the mock replacment
	if m, ok := mocks[nodeKey]; ok {
		mockSql, err := mockToSql(m)
		if err != nil {
			return Replacement{}, err
		}
		r := Replacement{
			TableFullName:  fullname,
			ReplaceSql:     mockSql.Sql,
			TableShortName: shortname}
		return r, nil
	}

	// if not a mock then recursively build query
	dependencies := currentNode.DependsOn["nodes"]
	node2sql := make(map[string]Replacement)
	for _, otherNodeKey := range dependencies {
		r, err := sql(manifest, otherNodeKey, mocks)
		if err != nil {
			return Replacement{}, err
		}
		node2sql[otherNodeKey] = r
	}

	sqlCode := currentNode.Compiledcode

	for _, replacement := range node2sql {
		sqlCode = Replace(sqlCode, replacement)

	}

	replacement := Replacement{ReplaceSql: sqlCode, TableFullName: fullname, TableShortName: shortname}
	return replacement, nil
}

/*
   Returns the SQL code for a Source.
   The returned SQL has replacement for the specified mocks.
*/
func sqlSource(manifest Manifest, sourceKey string, mocks map[string]Mock) (Replacement, error) {

	source := manifest.Sources[sourceKey]
	if m, ok := mocks[sourceKey]; ok {
		mockSql, err := mockToSql(m)
		if err != nil {
			return Replacement{}, err
		}
		r := Replacement{
			TableFullName:  source.RelationName,
			ReplaceSql:     mockSql.Sql,
			TableShortName: source.Name}
		return r, nil
	}
	errors.New("something")
	return Replacement{}, nil //errors.New(fmt.Sprintf("%v not mocked", sourceKey))
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

/*
   Represents a Mock as SQL
*/
type SQLMock struct {
	Sql     string
	Columns []string
}

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

/*
   Returns the SQL code for a model/source.
   The SQL retunred code has all mocked models/sources replaced for the data the mocks contained
*/
func sql(manifest Manifest, nodeKey string, mocks map[string]Mock) (Replacement, error) {

	if isModel(nodeKey) {
		return sqlModel(manifest, nodeKey, mocks)
	} else {
		return sqlSource(manifest, nodeKey, mocks)
	}
}

/*
   returns a Test structure given a filepath
*/
func parseTest(path string) (Test, error) {

	jsonFile, err := os.Open(path)
	if err != nil {
		return Test{}, err
	}
	bytes, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return Test{}, err
	}
	test := Test{}
	if err := json.Unmarshal(bytes, &test); err != nil {
		return Test{}, err
	}
	return test, nil
}

/*
   Given a folder returns a list of Test structs
*/
func parseFolder(path string) ([]Test, error) {

	files, err := ioutil.ReadDir(path)
	tests := []Test{}
	if err != nil {
		return []Test{}, err
	}
	for _, f := range files {
		fullPath := filepath.Join(path, f.Name())
		test, err := parseTest(fullPath)
		if err != nil {
			return []Test{}, err
		}
		tests = append(tests, test)

	}
	return []Test{}, nil
}

/*
   Given the SQL code of a model and an Expected Output mock,
   This function returns a SQL  query which asserts that the output table of SQL is equal to the data contained in the mock
*/
func assertSQLCode(sql string, output Mock) (string, error) {

	mockedSql, err := mockToSql(output)
	if err != nil {
		return "", err
	}
	columns := strings.Join(mockedSql.Columns, ",")
	return fmt.Sprintf("SELECT %s FROM( %s ) \n  EXCEPT DISTINCT \n SELECT %s FROM (%s)", columns, sql, columns, mockedSql.Sql), nil
}

/*
   Given a Test it generates the SQL code that mocks data, run the needed logic and asserts the output data
*/

func GenerateTestSQL(t Test, m Manifest) (string, error) {

	replacement, err := sql(m, t.Model, t.Mocks)
	if err != nil {
		return "", nil
	}
	sql, err := assertSQLCode(replacement.ReplaceSql, t.Output)
	if err != nil {
		return "", err
	}
	return sql, nil
}

func main() {
	m := parseManifest("target/manifest.json")
	t2, err := parseTest("test.json")
	if err != nil {
		panic(err)
	}
	sqlCode, err := GenerateTestSQL(t2, m)
	if err != nil {
		panic(err)
	}
	fmt.Println("")
	fmt.Println("LAST RESULT")
	fmt.Println(fmt.Sprintf("%v", sqlCode))
}

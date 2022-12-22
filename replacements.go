package dbtest

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

/* Describing Tests as structures */
type Mock struct {
	Filepath string            `json:"filepath"`
	Types    map[string]string `json:"types"`
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

type Replacement struct {
	TableFullName  string
	ReplaceSql     string
	TableShortName string
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
	return Replacement{}, errors.New(fmt.Sprintf("%v not mocked", sourceKey))
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
	if sqlCode == "" {
		return Replacement{}, errors.New(fmt.Sprintf("Node: %s has empty CompiledSql. Please make sure your manifest file is compiled by running `dbt compile`", nodeKey))
	}
	for _, replacement := range node2sql {
		sqlCode = Replace(sqlCode, replacement)

	}

	replacement := Replacement{ReplaceSql: sqlCode, TableFullName: fullname, TableShortName: shortname}
	return replacement, nil
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
		return "", err
	}
	sql, err := assertSQLCode(replacement.ReplaceSql, t.Output)
	if err != nil {
		return "", err
	}
	return sql, nil
}

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

type Mock struct {
	name   string
	model  bool
	source bool
}

type Output struct {
	name     string
	filetype string
}

type Test struct {
	name   string
	model  string
	mocks  []Mock
	output Output
}

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
	return strings.HasPrefix(nodeKey, "model")
}

func Replace(sql string, replacement Replacement) string {
	regexWithAlias, err := regexp.Compile(fmt.Sprintf(`(?i)%s\sas\s([A-z0-9_]+)\s`, replacement.TableFullName))
	if err != nil {
		panic(err)
	}
	useExistingAlias := fmt.Sprintf("(%s) AS $1 ", replacement.ReplaceSql)
	// replace all "TABLENAME as X" in current sql
	fmt.Println(regexWithAlias)
	fmt.Println("MATCHES")
	fmt.Println(regexWithAlias.FindAllString(sql, -1))
	newSql := regexWithAlias.ReplaceAllString(sql, useExistingAlias)
	createAlias := fmt.Sprintf("(%s) AS %s", replacement.ReplaceSql, replacement.TableShortName)
	// replace all "TABLENAME" in current sql
	newSql = strings.ReplaceAll(newSql, fmt.Sprintf("%s", replacement.TableFullName), createAlias)
	return newSql
}

func sqlModel(manifest Manifest, nodeKey string, mocks []Mock) (Replacement, error) {
	currentNode := manifest.Nodes[nodeKey]
	dependencies := currentNode.DependsOn["nodes"]
	fmt.Println("-------")
	fmt.Println(fmt.Sprintf("nodekey: %s", nodeKey))
	fmt.Println(fmt.Sprintf("deps %v", dependencies))
	fmt.Println("-------")
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
	fullname := fmt.Sprintf("`%s`.`%s`.`%s`", currentNode.Database, currentNode.Schema, currentNode.Name)
	shortname := currentNode.Alias
	replacement := Replacement{ReplaceSql: sqlCode, TableFullName: fullname, TableShortName: shortname}
	fmt.Println("Returning")
	fmt.Println(replacement)
	return replacement, nil
}

func sqlSource(manifest Manifest, sourceKey string, mocks []Mock) (Replacement, error) {
	// if source is in mocks then return mock, otherwise keep as it is
	source := manifest.Sources[sourceKey]
	r := Replacement{
		TableFullName:  source.RelationName,
		ReplaceSql:     "select 1 as id",
		TableShortName: source.Name}
	return r, nil
}

func sql(manifest Manifest, nodeKey string, mocks []Mock) (Replacement, error) {
	if isModel(nodeKey) {
		return sqlModel(manifest, nodeKey, mocks)
	} else {
		return sqlSource(manifest, nodeKey, mocks)
	}
}

func main() {
	test := Test{
		name:  "dummy_test",
		model: "dummy_model",
		mocks: []Mock{
			Mock{name: "mock1", model: true},
		},
		output: Output{name: "first check", filetype: "csv"},
	}
	m := parseManifest("manifest.json")

	replacement, err := sql(m, "model.data_feeds.liq_evals_by_asset", test.mocks)

	if err != nil {
		panic(err)
	}
	fmt.Println("")
	fmt.Println("LAST RESULT")
	fmt.Println(fmt.Sprintf("%v", replacement.ReplaceSql))
}

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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
	UniqueId     string                 `json:"unique_id"`
	Fqn          []string               `json:"fqn"`
	RelationName string                 `json:"relation_name"`
	X            map[string]interface{} `json:"-"`
}

type Node struct {
	Compiledcode string                 `json:"compiled_code"`
	DependsOn    map[string][]string    `json:"depends_on"`
	X            map[string]interface{} `json:"-"`
}

type Manifest struct {
	Nodes   map[string]Node        `json:"nodes"`
	Sources map[string]Source      `json:"sources"`
	X       map[string]interface{} `json:"-"`
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

func sql(manifest Manifest, nodeKey string, mocks[]Mock) (string, error) {
	currentNode := manifest.Nodes[nodeKey]
	dependencies := currentNode.DependsOn["nodes"]
	fmt.Println("-------")
	fmt.Println(fmt.Sprintf("nodekey: %s", nodeKey))
	fmt.Println(fmt.Sprintf("deps %v", dependencies))
	fmt.Println("-------")
	node2sql := make(map[string]string)
	for _, otherNodeKey := range dependencies {
		sqlCode, err := sql(manifest, otherNodeKey, mocks)
		if err!=nil {
			return "", err
		}
		node2sql[otherNodeKey] = sqlCode
	}

	sqlCode := currentNode.Compiledcode
	
	for _, newCode := range node2sql {
		dummy := newCode + ".."
		sqlCode = strings.ReplaceAll(sqlCode, "someoldtablename", dummy)
		
	}
	// replace table in current compiled code
	fmt.Println(sqlCode)
	
	return sqlCode, nil
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
	finalCode, err := sql(m, "model.data_feeds.volume_by_asset_latest", test.mocks)
	if err!=nil{
		panic(err)
	}
	fmt.Println(finalCode)	
}

package dbtmock

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// returns a Manifest structure out of a .json file
func ParseManifest(path string) Manifest {

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

/*
   returns a Test structure given a filepath
*/
func ParseTest(path string) (Test, error) {

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
func ParseFolder(path string) ([]Test, error) {

	files, err := ioutil.ReadDir(path)
	tests := []Test{}
	if err != nil {
		return []Test{}, err
	}
	for _, f := range files {

		fullPath := filepath.Join(path, f.Name())
		fmt.Println(fullPath)
		test, err := ParseTest(fullPath)
		if err != nil {
			return []Test{}, err
		}
		tests = append(tests, test)

	}
	return tests, nil
}

func SaveSQL(path string, sql string) error {
	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}
	data := []byte(sql)
	err = ioutil.WriteFile(path, data, 0644)
	if err != nil {
		return err
	}
	return nil
}

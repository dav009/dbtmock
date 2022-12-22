package main

import (
	"fmt"

	"github.com/dav009/dbtest"
)

func main() {
	m := dbtest.ParseManifest("target/manifest.json")
	t, err := dbtest.ParseTest("test.json")
	if err != nil {
		panic(err)
	}
	sqlCode, err := dbtest.GenerateTestSQL(t, m)
	if err != nil {
		panic(err)
	}
	fmt.Println("")
	fmt.Println(fmt.Sprintf("%v", sqlCode))
}

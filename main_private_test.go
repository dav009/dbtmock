package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplaceNoPreviousAlias(t *testing.T) {
	replacement := Replacement{
		TableFullName:  "`one`.`two`.`three`",
		ReplaceSql:     "select * from x",
		TableShortName: "new_table_name",
	}
	replaced := Replace("from `one`.`two`.`three` do something", replacement)
	assert.Equal(t, replaced, "from (select * from x) AS new_table_name do something")
}

func TestReplacePreviousAlias(t *testing.T) {
	replacement := Replacement{
		TableFullName:  "`one`.`two`.`three`",
		ReplaceSql:     "select * from x",
		TableShortName: "new_table_name",
	}
	replaced := Replace("from `one`.`two`.`three` AS ALIAS1 do something", replacement)
	assert.Equal(t, replaced, "from (select * from x) AS ALIAS1 do something")
}

func TestMockToSql(t *testing.T) {
	m := Mock{Filepath: "sample.csv"}
	sql := mockToSql(m)
	expectedSQL1 := "SELECT \"something\" AS column1, \"1.0\" AS column2, \"100\" AS column3"
	expectedSQL2 := "SELECT \"something2\" AS column1, \"2.0\" AS column2, \"200\" AS column3"
	expectedSQL3 := "SELECT \"something3\" AS column1, \"3.0\" AS column2, \"300\" AS column3"
	assert.True(t, strings.Contains(sql, expectedSQL1))
	assert.True(t, strings.Contains(sql, expectedSQL2))
	assert.True(t, strings.Contains(sql, expectedSQL3))
}

func TestMockToSqlWithTypes(t *testing.T) {
	m := Mock{Filepath: "sample.csv", Types: map[string]string{"column2": "INT64"}}
	sql := mockToSql(m)
	expectedSQL1 := "SELECT \"something\" AS column1, CAST(\"1.0\" AS INT64) AS column2, \"100\" AS column3"
	expectedSQL2 := "SELECT \"something2\" AS column1, CAST(\"2.0\" AS INT64) AS column2, \"200\" AS column3"
	expectedSQL3 := "SELECT \"something3\" AS column1, CAST(\"3.0\" AS INT64) AS column2, \"300\" AS column3"
	assert.True(t, strings.Contains(sql, expectedSQL1))
	assert.True(t, strings.Contains(sql, expectedSQL2))
	assert.True(t, strings.Contains(sql, expectedSQL3))
}

func TestParseJson(t *testing.T) {

	fmt.Println("parsing")
	test, err := parseTest("test.json")
	assert.Nil(t, err)
	expectedTest := Test{
		Name:   "dummy_test",
		Output: Mock{Name: "something"},
		Model:  "dummy_model",
		Mocks: map[string]Mock{
			"something": Mock{
				Name:     "mock1.mock",
				Filepath: "something.csv",
				Types: map[string]string{
					"c1": "int64",
				},
			},
		},
	}
	assert.Equal(t, test, expectedTest)
}

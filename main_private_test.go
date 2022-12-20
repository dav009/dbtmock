package main

import (
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

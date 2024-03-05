package clickhouse

import (
	"testing"

	"yunion.io/x/sqlchemy"
	"yunion.io/x/sqlchemy/backends/tests"
)

func TestQuery(t *testing.T) {
	t.Run("query all fields", func(t *testing.T) {
		tests.BackendTestReset(sqlchemy.ClickhouseBackend)
		q := tests.GetTestTable().Query()
		want := "SELECT `t1`.`col0` AS `col0`, `t1`.`col1` AS `col1`, `t1`.`col2` AS `col2` FROM `test` AS `t1`"
		tests.AssertGotWant(t, q.String(), want)
	})

	t.Run("query regexp field", func(t *testing.T) {
		tests.BackendTestReset(sqlchemy.ClickhouseBackend)
		testTable := tests.GetTestTable()
		q := testTable.Query(testTable.Field("col0")).Regexp("col1", "^ab$")
		want := "SELECT `t1`.`col0` AS `col0` FROM `test` AS `t1` WHERE match(`t1`.`col1`,  ? )"
		tests.AssertGotWant(t, q.String(), want)
	})
}

package mysql

import (
	"testing"

	"yunion.io/x/sqlchemy"
)

type testQueryTable struct {
	Col0 string
	Col1 int
}

var (
	testTableSpec *sqlchemy.STableSpec
	testTable     *sqlchemy.STable
)

func testReset() {
	sqlchemy.ResetTableID()

	sqlchemy.SetDefaultDB(nil)
	testTableSpec = sqlchemy.NewTableSpecFromStruct(testQueryTable{}, "test")
	testTable = testTableSpec.Instance()
}

func testGotWant(t *testing.T, got, want string) {
	if got != want {
		t.Fatalf("\ngot:\n%s\nwant:\n%s\n", got, want)
	}
}

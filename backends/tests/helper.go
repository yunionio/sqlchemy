package tests

import (
	"testing"

	"yunion.io/x/sqlchemy"
)

type testQueryTable struct {
	Col0 string `width:"128" charset:"utf8" nullable:"false"`
	Col1 int
	Col2 string `charset:"utf8" length:"medium" nullable:"false"`
}

var (
	testTableSpec *sqlchemy.STableSpec
	testTable     *sqlchemy.STable
)

func GetTestTableSpec() *sqlchemy.STableSpec {
	return testTableSpec
}

func GetTestTable() *sqlchemy.STable {
	return testTable
}

func BackendTestReset(backend sqlchemy.DBBackendName) {
	sqlchemy.ResetTableID()

	sqlchemy.SetDBWithNameBackend(nil, sqlchemy.DefaultDB, backend)
	testTableSpec = sqlchemy.NewTableSpecFromStruct(testQueryTable{}, "test")
	testTable = testTableSpec.Instance()
}

func AssertGotWant(t *testing.T, got, want string) {
	if got != want {
		t.Fatalf("\ngot:\n%s\nwant:\n%s\n", got, want)
	}
}

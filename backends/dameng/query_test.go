// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dameng

import (
	"testing"

	"yunion.io/x/sqlchemy"
	"yunion.io/x/sqlchemy/backends/tests"
)

var (
	testTable   *sqlchemy.STable
	testGotWant = tests.AssertGotWant
)

func testReset() {
	tests.BackendTestReset(sqlchemy.DamengBackend)
	testTable = tests.GetTestTable()
}

func TestQuery(t *testing.T) {
	t.Run("query all fields", func(t *testing.T) {
		testReset()
		q := testTable.Query()
		want := `SELECT "t1"."col0" AS "col0", "t1"."col1" AS "col1", "t1"."col2" AS "col2" FROM "test" AS "t1"`
		testGotWant(t, q.String(), want)
	})

	t.Run("query selected fields", func(t *testing.T) {
		testReset()
		q := testTable.Query(testTable.Field("col0")).Equals("col1", 100)
		want := `SELECT "t1"."col0" AS "col0" FROM "test" AS "t1" WHERE "t1"."col1" =  ? `
		testGotWant(t, q.String(), want)
	})

	t.Run("query regexp field", func(t *testing.T) {
		testReset()
		q := testTable.Query(testTable.Field("col0")).Regexp("col1", "^ab$")
		want := `SELECT "t1"."col0" AS "col0" FROM "test" AS "t1" WHERE "t1"."col1" REGEXP  ? `
		testGotWant(t, q.String(), want)
	})

	t.Run("query selected fields from subquery", func(t *testing.T) {
		testReset()
		q := testTable.Query().SubQuery().Query(testTable.Field("col0")).Equals("col1", 100)
		want := `SELECT "t1"."col0" AS "col0" FROM (SELECT "t1"."col1" AS "col1" FROM "test" AS "t1") AS "t2" WHERE "t2"."col1" =  ? `
		testGotWant(t, q.String(), want)
	})

	t.Run("query union", func(t *testing.T) {
		testReset()
		q1 := testTable.Query(testTable.Field("col0")).Equals("col1", 100)
		q2 := testTable.Query(testTable.Field("col0")).Equals("col1", 200)
		uq := sqlchemy.Union(q1, q2)
		q := uq.Query()
		want := `SELECT "t2"."col0" AS "col0" FROM (SELECT "t3"."col0" AS "col0" FROM (SELECT "t1"."col0" AS "col0" FROM "test" AS "t1" WHERE "t1"."col1" =  ? ) AS "t3" UNION SELECT "t4"."col0" AS "col0" FROM (SELECT "t1"."col0" AS "col0" FROM "test" AS "t1" WHERE "t1"."col1" =  ? ) AS "t4") AS "t2"`
		testGotWant(t, q.String(), want)
	})

	t.Run("query order by SUM func", func(t *testing.T) {
		testReset()
		q := testTable.Query(sqlchemy.SUM("total", testTable.Field("col1")), testTable.Field("col0")).GroupBy(testTable.Field("col0"))
		q = q.Asc(q.Field("total"))
		want := `SELECT SUM("t1"."col1") AS "total", "t1"."col0" AS "col0" FROM "test" AS "t1" GROUP BY "t1"."col0" ORDER BY "total" ASC`
		testGotWant(t, q.String(), want)
	})

	t.Run("query GROUP_CONCAT2 func", func(t *testing.T) {
		testReset()
		q := testTable.Query(sqlchemy.SUM("total", testTable.Field("col1")), sqlchemy.GROUP_CONCAT("all_col2", testTable.Field("col2")), testTable.Field("col0")).GroupBy(testTable.Field("col0"))
		q = q.Asc(q.Field("total"))
		want := `SELECT SUM("t1"."col1") AS "total", WM_CONCAT("t1"."col2") AS "all_col2", "t1"."col0" AS "col0" FROM "test" AS "t1" GROUP BY "t1"."col0" ORDER BY "total" ASC`
		testGotWant(t, q.String(), want)
	})

	t.Run("query INET_ATON func", func(t *testing.T) {
		testReset()
		q := testTable.Query(testTable.Field("col1"), sqlchemy.INET_ATON(testTable.Field("col0")).Label("ipaddr"))
		want := `SELECT "t1"."col1" AS "col1", TO_NUMBER(SUBSTR("t1"."col0",1,INSTR("t1"."col0",'.')-1))*POWER(256,3)+TO_NUMBER(SUBSTR("t1"."col0",INSTR("t1"."col0",'.')+1,INSTR("t1"."col0",'.',1,2)-INSTR("t1"."col0",'.')-1))*POWER(256,2)+TO_NUMBER(SUBSTR("t1"."col0",INSTR("t1"."col0",'.',1,2)+1,INSTR("t1"."col0",'.',1,3)-INSTR("t1"."col0",'.',1,2)-1))*256+TO_NUMBER(SUBSTR("t1"."col0",INSTR("t1"."col0",'.',1,3)+1)) AS "ipaddr" FROM "test" AS "t1"`
		testGotWant(t, q.String(), want)
	})

	t.Run("query INET_ATON func by group", func(t *testing.T) {
		testReset()
		q := testTable.Query(testTable.Field("col1").Label("number"), sqlchemy.INET_ATON(testTable.Field("col0")).Label("ipaddr"), sqlchemy.NewConstField(123456).Label("gateway")).GroupBy(testTable.Field("col0"))
		want := `SELECT MAX("t1"."col1") AS "number", MAX(TO_NUMBER(SUBSTR("t1"."col0",1,INSTR("t1"."col0",'.')-1))*POWER(256,3)+TO_NUMBER(SUBSTR("t1"."col0",INSTR("t1"."col0",'.')+1,INSTR("t1"."col0",'.',1,2)-INSTR("t1"."col0",'.')-1))*POWER(256,2)+TO_NUMBER(SUBSTR("t1"."col0",INSTR("t1"."col0",'.',1,2)+1,INSTR("t1"."col0",'.',1,3)-INSTR("t1"."col0",'.',1,2)-1))*256+TO_NUMBER(SUBSTR("t1"."col0",INSTR("t1"."col0",'.',1,3)+1))) AS "ipaddr", 123456 AS "gateway" FROM "test" AS "t1" GROUP BY "t1"."col0"`
		testGotWant(t, q.String(), want)
	})
}

func TestCountQuery(t *testing.T) {
	testReset()

	q := testTable.Query()
	q.GroupBy("col0")
	q.Limit(8)
	q.Offset(10)
	cq := q.CountQuery()
	want := `SELECT COUNT(*) AS "count" FROM (SELECT "t1"."col0" AS "col0", MAX("t1"."col1") AS "col1", MAX("t1"."col2") AS "col2" FROM "test" AS "t1" GROUP BY "t1"."col0") AS "t2"`
	testGotWant(t, cq.String(), want)
}

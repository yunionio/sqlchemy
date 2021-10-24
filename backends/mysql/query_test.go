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

package mysql

import (
	"testing"

	"yunion.io/x/sqlchemy"
)

func TestQuery(t *testing.T) {
	t.Run("query all fields", func(t *testing.T) {
		testReset()
		q := testTable.Query()
		want := "SELECT `t1`.`col0`, `t1`.`col1` FROM `test` AS `t1`"
		testGotWant(t, q.String(), want)
	})

	t.Run("query selected fields", func(t *testing.T) {
		testReset()
		q := testTable.Query(testTable.Field("col0")).Equals("col1", 100)
		want := "SELECT `t1`.`col0` FROM `test` AS `t1` WHERE `t1`.`col1` = ( ? )"
		testGotWant(t, q.String(), want)
	})

	t.Run("query selected fields from subquery", func(t *testing.T) {
		testReset()
		q := testTable.Query().SubQuery().Query(testTable.Field("col0")).Equals("col1", 100)
		want := "SELECT `t1`.`col0` FROM (SELECT `t1`.`col1` FROM `test` AS `t1`) AS `t2` WHERE `t2`.`col1` = ( ? )"
		testGotWant(t, q.String(), want)
	})

	t.Run("query union", func(t *testing.T) {
		testReset()
		q1 := testTable.Query(testTable.Field("col0")).Equals("col1", 100)
		q2 := testTable.Query(testTable.Field("col0")).Equals("col1", 200)
		uq := sqlchemy.Union(q1, q2)
		q := uq.Query()
		want := "SELECT `t2`.`col0` FROM (SELECT `t1`.`col0` FROM `test` AS `t1` WHERE `t1`.`col1` = ( ? ) UNION SELECT `t1`.`col0` FROM `test` AS `t1` WHERE `t1`.`col1` = ( ? )) AS `t2`"
		testGotWant(t, q.String(), want)
	})

	t.Run("query order by SUM func", func(t *testing.T) {
		testReset()
		q := testTable.Query(sqlchemy.SUM("total", testTable.Field("col1")), testTable.Field("col0")).GroupBy(testTable.Field("col0"))
		q = q.Asc(q.Field("total"))
		want := "SELECT SUM(`t1`.`col1`) AS `total`, `t1`.`col0` FROM `test` AS `t1` GROUP BY `t1`.`col0` ORDER BY `total` ASC"
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
	want := "SELECT COUNT(*) AS `count` FROM (SELECT `t1`.`col0`, `t1`.`col1` FROM `test` AS `t1` GROUP BY `t1`.`col0`) AS `t2`"
	testGotWant(t, cq.String(), want)
}

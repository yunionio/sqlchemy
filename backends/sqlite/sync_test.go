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

package sqlite

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"yunion.io/x/sqlchemy"
)

func TestSync(t *testing.T) {
	type TableStruct1 struct {
		Id     uint64 `auto_increment:"true"`
		Name   string `width:"64" charset:"utf8"`
		Age    int    `nullable:"true" default:"12"`
		IsMale *bool  `nullable:"false" default:"true"`
	}
	type TableStruct2 struct {
		Id     uint64 `auto_increment:"true"`
		Name   string `width:"128" charset:"utf8"`
		Age    uint   `nullable:"true" default:"10"`
		Gender string `width:"8" nullable:"false" default:"male"`
	}
	dbConn, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Errorf("open sqlite memory db fail: %s", err)
		return
	}
	defer dbConn.Close()
	sqlchemy.SetDBWithNameBackend(dbConn, sqlchemy.DefaultDB, sqlchemy.SQLiteBackend)
	ts1 := sqlchemy.NewTableSpecFromStruct(TableStruct1{}, "table1")
	ts2 := sqlchemy.NewTableSpecFromStruct(TableStruct2{}, "table1")

	changes := sqlchemy.STableChanges{}
	changes.RemoveColumns, changes.UpdatedColumns, changes.AddColumns = sqlchemy.DiffCols(ts2.Name(), ts1.Columns(), ts2.Columns())
	backend := &SSqliteBackend{}
	sqls := backend.CommitTableChangeSQL(ts2, changes)
	want := []string{
		"ALTER TABLE `table1` ADD COLUMN `gender` TEXT NOT NULL DEFAULT 'male' COLLATE NOCASE",
		"PRAGMA encoding=\"UTF-8\"",
		"CREATE TABLE IF NOT EXISTS `table1_tmp` (\n`age` INTEGER DEFAULT 10,\n`gender` TEXT NOT NULL DEFAULT 'male' COLLATE NOCASE,\n`id` INTEGER PRIMARY KEY NOT NULL,\n`name` TEXT COLLATE NOCASE\n)",
		"INSERT INTO `table1_tmp` SELECT `age`, `gender`, `id`, `name` FROM `table1`",
		"ALTER TABLE `table1` RENAME TO `table1_old`",
		"ALTER TABLE `table1_tmp` RENAME TO `table1`",
	}
	if !reflect.DeepEqual(sqls, want) {
		t.Errorf("Expect: %s", want)
		t.Errorf("Got: %s", sqls)
	}
}

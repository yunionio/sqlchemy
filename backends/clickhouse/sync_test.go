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

package clickhouse

import (
	"reflect"
	"testing"
	"time"

	"yunion.io/x/sqlchemy"
)

func TestSync(t *testing.T) {
	type TableStruct1 struct {
		Id        uint64    `auto_increment:"true"`
		Name      string    `width:"64" charset:"utf8"`
		Age       int       `nullable:"true" default:"12"`
		IsMale    *bool     `nullable:"false" default:"true"`
		CreatedAt time.Time `created_at:"true" clickhouse_ttl:"3m"`
	}
	type TableStruct2 struct {
		Id        uint64    `auto_increment:"true"`
		Name      string    `width:"128" charset:"utf8"`
		Age       uint      `nullable:"true" default:"12"`
		Gender    string    `width:"8" nullable:"false" default:"male"`
		CreatedAt time.Time `created_at:"true" clickhouse_ttl:"6m"`
	}
	type TableStruct3 struct {
		Id        uint64    `auto_increment:"true"`
		Name      string    `width:"128" charset:"utf8"`
		Age       uint      `nullable:"true" default:"12"`
		Gender    string    `width:"8" nullable:"false" default:"male"`
		CreatedAt time.Time `created_at:"true"`
	}

	sqlchemy.SetDBWithNameBackend(nil, sqlchemy.DefaultDB, sqlchemy.ClickhouseBackend)

	cases := []struct {
		ts1  *sqlchemy.STableSpec
		ts2  *sqlchemy.STableSpec
		want []string
	}{
		{
			ts1: sqlchemy.NewTableSpecFromStruct(TableStruct1{}, "table1"),
			ts2: sqlchemy.NewTableSpecFromStruct(TableStruct2{}, "table1"),
			want: []string{
				"ALTER TABLE `table1` MODIFY COLUMN `age` Nullable(UInt32) DEFAULT 12, ADD COLUMN `gender` String DEFAULT 'male', MODIFY TTL `created_at` + INTERVAL 6 MONTH;",
			},
		},
		{
			ts1: sqlchemy.NewTableSpecFromStruct(TableStruct2{}, "table1"),
			ts2: sqlchemy.NewTableSpecFromStruct(TableStruct3{}, "table1"),
			want: []string{
				"ALTER TABLE `table1` REMOVE TTL;",
			},
		},
	}

	for i, c := range cases {
		changes := sqlchemy.STableChanges{}
		changes.RemoveColumns, changes.UpdatedColumns, changes.AddColumns = sqlchemy.DiffCols(c.ts2.Name(), c.ts1.Columns(), c.ts2.Columns())
		changes.OldColumns = c.ts1.Columns()
		backend := &SClickhouseBackend{}
		sqls := backend.CommitTableChangeSQL(c.ts2, changes)
		if !reflect.DeepEqual(sqls, c.want) {
			t.Errorf("[%d] Expect: %s Got: %s", i, c.want, sqls)
		}
	}
}

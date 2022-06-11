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

package sqlchemy

import (
	"reflect"
	"testing"
	"time"
)

func TestDiffCols(t *testing.T) {
	SetupMockDatabaseBackend()

	type TableStruct struct {
		Id        int       `json:"id" primary:"true"`
		Name      string    `width:"16"`
		Age       int       `nullable:"true"`
		IsMale    bool      `nullalbe:"true"`
		CreatedAt time.Time `created_at:"true"`
		UpdatedAt time.Time `updated_at:"true"`
		Version   int32     `auto_version:"true"`
		DispName  string    `width:"10"`
		Dept      string    `width:"10"`
	}
	table := NewTableSpecFromStruct(TableStruct{}, "testtable")

	type TableStruct2 struct {
		Id        int       `json:"id" primary:"true"`
		Name      string    `width:"16"`
		Age       int       `nullable:"true"`
		CreatedAt time.Time `created_at:"true"`
		UpdatedAt time.Time `updated_at:"true"`
		Version   int64     `auto_version:"true"`
		DispName  string    `width:"20"`
	}
	table2 := NewTableSpecFromStruct(TableStruct2{}, "testtable")

	cases := []struct {
		cols1  []IColumnSpec
		cols2  []IColumnSpec
		remove int
		update int
		add    int
	}{
		{
			cols1: table.Columns(),
			cols2: table.Columns(),
		},
		{
			cols1:  table.Columns(),
			cols2:  table2.Columns(),
			remove: 2,
			update: 1,
		},
		{
			cols1:  table2.Columns(),
			cols2:  table.Columns(),
			add:    2,
			update: 1,
		},
	}
	for _, c := range cases {
		remove, update, add := DiffCols("testtable", c.cols1, c.cols2)
		t.Logf("remove %d update %d add %d", len(remove), len(update), len(add))
		if len(remove) != c.remove {
			t.Errorf("remove want %d got %d", c.remove, len(remove))
		}
		if len(update) != c.update {
			t.Errorf("update want %d got %d", c.update, len(update))
		}
		if len(add) != c.add {
			t.Errorf("add want %d got %d", c.add, len(add))
		}
	}
}

func TestDiffIndex(t *testing.T) {
	cases := []struct {
		index1 []STableIndex
		index2 []STableIndex
		remove []STableIndex
		add    []STableIndex
	}{
		{
			index1: []STableIndex{
				NewTableIndex(nil, []string{"name"}, false),
			},
			index2: []STableIndex{
				NewTableIndex(nil, []string{"name", "age"}, false),
			},
			remove: []STableIndex{
				NewTableIndex(nil, []string{"name"}, false),
			},
			add: []STableIndex{
				NewTableIndex(nil, []string{"name", "age"}, false),
			},
		},
		{
			index1: []STableIndex{
				NewTableIndex(nil, []string{"name"}, false),
			},
			index2: []STableIndex{
				NewTableIndex(nil, []string{"name"}, false),
			},
			remove: []STableIndex{},
			add:    []STableIndex{},
		},
	}
	for _, c := range cases {
		add, remove := diffIndexes(c.index1, c.index2)
		if !reflect.DeepEqual(add, c.add) {
			t.Errorf("Add got %#v want %#v", add, c.add)
		}
		if !reflect.DeepEqual(remove, c.remove) {
			t.Errorf("Remove got %#v want %#v", remove, c.remove)
		}
	}
}

func TestSync(t *testing.T) {
	type TableStruct1 struct {
		Id     uint64 `auto_increment:"true"`
		Name   string `width:"64" charset:"utf8" index:"true"`
		Age    int    `nullable:"true" default:"12"`
		IsMale *bool  `nullable:"false" default:"true"`
	}
	type TableStruct2 struct {
		Id     uint64 `auto_increment:"true"`
		Name   string `width:"128" charset:"utf8"`
		Age    uint   `nullable:"true" default:"10" index:"true"`
		Gender string `width:"8" nullable:"false" default:"male"`
	}

	SetupMockDatabaseBackend()
	ts1 := NewTableSpecFromStruct(TableStruct1{}, "table1")
	ts2 := NewTableSpecFromStruct(TableStruct2{}, "table1")

	changes := STableChanges{}
	changes.RemoveColumns, changes.UpdatedColumns, changes.AddColumns = DiffCols(ts2.Name(), ts1.Columns(), ts2.Columns())
	changes.OldColumns = ts1.Columns()
	backend := &sMockBackend{}
	sqls := backend.CommitTableChangeSQL(ts2, changes)
	want := []string{}
	if !reflect.DeepEqual(sqls, want) {
		t.Errorf("Expect: %s", want)
		t.Errorf("Got: %s", sqls)
	}
}

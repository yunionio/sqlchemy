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
	"testing"
	"time"
)

func TestSyncTable(t *testing.T) {
	SetupMockDatabaseBackend()

	type TableStruct struct {
		Id        int       `json:"id" primary:"true"`
		Name      string    `width:"16"`
		Age       int       `nullable:"true"`
		IsMale    bool      `nullalbe:"true"`
		CreatedAt time.Time `created_at:"true"`
		UpdatedAt time.Time `updated_at:"true"`
		Version   int64     `auto_version:"true"`
	}
	table := NewTableSpecFromStruct(TableStruct{}, "testtable")

	type TableStruct2 struct {
		Id        int       `json:"id" primary:"true"`
		Name      string    `width:"16"`
		Age       int       `nullable:"true"`
		CreatedAt time.Time `created_at:"true"`
		UpdatedAt time.Time `updated_at:"true"`
		Version   int64     `auto_version:"true"`
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
			remove: 1,
		},
		{
			cols1: table2.Columns(),
			cols2: table.Columns(),
			add:   1,
		},
	}
	for _, c := range cases {
		remove, update, add := diffCols("testtable", c.cols1, c.cols2)
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

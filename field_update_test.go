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

func TestUpdateFieldSql(t *testing.T) {
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
	dt := TableStruct{
		Id: 123456,
	}
	cases := []struct {
		fields  map[string]interface{}
		wantSql string
		vars    int
	}{
		{
			fields: map[string]interface{}{
				"name":    "John",
				"age":     23,
				"is_male": false,
			},
			wantSql: "UPDATE `testtable` SET `name` = ?, `age` = ?, `is_male` = ?, `version` = `version` + 1, `updated_at` = ? WHERE `id` = ?",
			vars:    5,
		},
		{
			fields: map[string]interface{}{
				"name": "John",
			},
			wantSql: "UPDATE `testtable` SET `name` = ?, `version` = `version` + 1, `updated_at` = ? WHERE `id` = ?",
			vars:    3,
		},
	}
	for _, c := range cases {
		results, err := table.updateFieldSql(&dt, c.fields, false)
		if err != nil {
			t.Errorf("updateFieldSql Error %s", err)
		} else {
			t.Logf("primary: %s", results.primaries)
			if results.Sql != c.wantSql {
				t.Errorf("want: %s got: %s", c.wantSql, results.Sql)
			}
			if len(results.Vars) != c.vars {
				t.Errorf("want vars: %d got %d", c.vars, len(results.Vars))
			}
		}
	}
}

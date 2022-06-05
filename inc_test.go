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

func TestIncrementalSQL(t *testing.T) {
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
		Id:  12345,
		Age: 2,
	}

	cases := []struct {
		opcode   string
		wantSQL  string
		wantVars int
	}{
		{
			opcode:   "+",
			wantSQL:  "UPDATE `testtable` SET `age` = `age` + ?, `version` = `version` + 1, `updated_at` = UTC_NOW() WHERE `id` = ?",
			wantVars: 2,
		},
		{
			opcode:   "-",
			wantSQL:  "UPDATE `testtable` SET `age` = `age` - ?, `version` = `version` + 1, `updated_at` = UTC_NOW() WHERE `id` = ?",
			wantVars: 2,
		},
	}
	for _, c := range cases {
		result, err := table.incrementInternalSql(&dt, c.opcode, nil)
		if err != nil {
			t.Fatalf("incrementInternalSql fail %s", err)
		}
		if c.wantSQL != result.Sql {
			t.Fatalf("SQL increment want: %s got %s", c.wantSQL, result.Sql)
		}
		if c.wantVars != len(result.Vars) {
			t.Fatalf("SQL increment want vars: %d got %d", c.wantVars, len(result.Vars))
		}
	}
}

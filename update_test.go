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
)

func TestUpdateSQL(t *testing.T) {
	setupMockDatabaseBackend()

	table := NewTableSpecFromStruct(TableStruct{}, "testtable")
	dt := TableStruct{
		Id:     12345,
		Name:   "john",
		Age:    20,
		IsMale: true,
	}
	session, err := table.prepareUpdate(&dt)
	if err != nil {
		t.Fatalf("prepareUpdate fail %s", err)
	}
	dt.Name = "johny"
	result, err := session.saveUpdateSql(&dt)
	if err != nil {
		t.Fatalf("saveUpdateSql fail %s", err)
	}
	want := "UPDATE `testtable` SET `name` = ?, `version` = `version` + 1, `updated_at` = ? WHERE `id` = ?"
	wantVars := 3
	if want != result.sql {
		t.Fatalf("SQL: want %s got %s", want, result.sql)
	}
	if wantVars != len(result.vars) {
		t.Fatalf("Vars want %d got %d", wantVars, len(result.vars))
	}
}

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

const (
	uuid = "bfaf21ec-861e-4a7d-8739-7139588f0e00"
)

type TableStruct struct {
	Id        int       `json:"id" primary:"true"`
	UserId    string    `width:"128" charset:"ascii" nullable:"false"`
	Name      string    `width:"16"`
	Age       int       `nullable:"true"`
	IsMale    bool      `nullalbe:"true"`
	CreatedAt time.Time `created_at:"true"`
	UpdatedAt time.Time `updated_at:"true"`
	Version   int64     `auto_version:"true"`
}

func (s *TableStruct) BeforeInsert() {
	s.UserId = uuid
}

func (s *TableStruct) BeforeUpdate() {
	if len(s.Name) > 16 {
		s.Name = s.Name[:14] + ".."
	}
}

func TestInsertSQL(t *testing.T) {
	setupMockDatabaseBackend()

	table := NewTableSpecFromStruct(TableStruct{}, "testtable")
	value := TableStruct{
		Id:     12345,
		Name:   "John",
		Age:    20,
		IsMale: true,
	}
	results, err := table.InsertSqlPrep(&value, false)
	if err != nil {
		t.Fatalf("insertSqlPref fail %s", err)
	}
	want := "INSERT INTO `testtable` (`id`, `user_id`, `name`, `age`, `is_male`, `created_at`, `updated_at`, `version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	wantVars := 8
	if results.Sql != want {
		t.Errorf("SQL: want %s got %s", want, results.Sql)
	}
	if len(results.Values) != wantVars {
		t.Errorf("VARs want %d got %d", wantVars, len(results.Values))
	}
	if value.UserId != uuid {
		t.Errorf("want %s got %s", uuid, value.UserId)
	}
}

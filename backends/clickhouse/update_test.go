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
	"testing"
	"time"

	"yunion.io/x/sqlchemy"
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

func TestUpdateSQL(t *testing.T) {
	sqlchemy.SetDBWithNameBackend(nil, sqlchemy.DefaultDB, sqlchemy.ClickhouseBackend)

	table := sqlchemy.NewTableSpecFromStruct(TableStruct{}, "testtable")
	dt := TableStruct{
		Id:     12345,
		Name:   "john",
		Age:    20,
		IsMale: true,
	}
	session, err := table.PrepareUpdate(&dt)
	if err != nil {
		t.Fatalf("prepareUpdate fail %s", err)
	}
	dt.Name = "johny"
	result, err := session.SaveUpdateSql(&dt)
	if err != nil {
		t.Fatalf("saveUpdateSql fail %s", err)
	}
	want := "ALTER TABLE `testtable` UPDATE `name` = ?, `version` = `version` + 1, `updated_at` = ? WHERE `id` = ?"
	wantVars := 3
	if want != result.Sql {
		t.Fatalf("SQL: want %s got %s", want, result.Sql)
	}
	if wantVars != len(result.Vars) {
		t.Fatalf("Vars want %d got %d", wantVars, len(result.Vars))
	}
}

type SMetadata struct {
	// 资源类型
	// example: network
	ObjType string `width:"40" charset:"ascii" index:"true" list:"user" get:"user"`

	// 资源ID
	// example: 87321a70-1ecb-422a-8b0c-c9aa632a46a7
	ObjId string `width:"88" charset:"ascii" index:"true" list:"user" get:"user"`

	// 资源组合ID
	// example: network::87321a70-1ecb-422a-8b0c-c9aa632a46a7
	Id string `width:"128" charset:"ascii" primary:"true" list:"user" get:"user"`

	// 标签KEY
	// exmaple: 部门
	Key string `width:"64" charset:"utf8" primary:"true" list:"user" get:"user"`

	// 标签值
	// example: 技术部
	Value string `charset:"utf8" list:"user" get:"user"`

	// 更新时间
	UpdatedAt time.Time `nullable:"false" updated_at:"true"`

	// 是否被删除
	Deleted bool `nullable:"false" default:"false" index:"true"`
}

func TestUpdatePrimaryKey(t *testing.T) {
	sqlchemy.SetDBWithNameBackend(nil, sqlchemy.DefaultDB, sqlchemy.ClickhouseBackend)

	table := sqlchemy.NewTableSpecFromStruct(SMetadata{}, "metadata_tbl")
	dt := SMetadata{
		ObjType: "server",
		ObjId:   "0911ae37-4bcd-4bdd-8942-1ab9a4280ab5",
		Id:      "server::0911ae37-4bcd-4bdd-8942-1ab9a4280ab5",
		Key:     "projname",
		Value:   "hwtest",
	}
	session, err := table.PrepareUpdate(&dt)
	if err != nil {
		t.Fatalf("prepareUpdate fail %s", err)
	}
	dt.Key = "projName"
	dt.Value = "testhw"
	result, err := session.SaveUpdateSql(&dt)
	if err != nil {
		t.Fatalf("saveUpdateSql fail %s", err)
	}
	want := "ALTER TABLE `metadata_tbl` UPDATE `key` = ?, `value` = ?, `updated_at` = ? WHERE `id` = ? AND `key` = ?"
	wantVars := 5
	if want != result.Sql {
		t.Fatalf("SQL: want %s got %s", want, result.Sql)
	}
	if wantVars != len(result.Vars) {
		t.Fatalf("Vars want %d got %d", wantVars, len(result.Vars))
	}
}

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

func TestDecodeSqlTypeString(t *testing.T) {
	t.Log(decodeSqlTypeString("VARCHAR(128)"))
	t.Log(decodeSqlTypeString("VARCHAR"))
	t.Log(decodeSqlTypeString("DECIMAL(10,2)"))
	t.Log(decodeSqlTypeString("TINYINT UNSIGNED"))
	t.Log(decodeSqlTypeString("INT UNSIGNED"))
}

func TestToColumnSpec(t *testing.T) {
	cases := []struct {
		info SSqlColumnInfo
	}{
		{
			info: SSqlColumnInfo{
				Field:     "created_at",
				Type:      "datetime",
				Collation: "NULL",
				Null:      "NO",
				Key:       "MUL",
				Default:   "NULL",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "updated_at",
				Type:      "datetime",
				Collation: "NULL",
				Null:      "NO",
				Key:       "",
				Default:   "NULL",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "update_version",
				Type:      "int(11)",
				Collation: "NULL",
				Null:      "NO",
				Key:       "",
				Default:   "0",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "update_version",
				Type:      "int unsigned",
				Collation: "NULL",
				Null:      "NO",
				Key:       "",
				Default:   "0",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "update_version",
				Type:      "int(10) unsigned",
				Collation: "NULL",
				Null:      "NO",
				Key:       "",
				Default:   "0",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "id",
				Type:      "varchar(128)",
				Collation: "ascii_general_ci",
				Null:      "NO",
				Key:       "PRI",
				Default:   "NULL",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "name",
				Type:      "varchar(128)",
				Collation: "utf8_general_ci",
				Null:      "NO",
				Key:       "MUL",
				Default:   "NULL",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "cmtbound",
				Type:      "float",
				Collation: "NULL",
				Null:      "YES",
				Key:       "",
				Default:   "1",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "is_sys_disk_store",
				Type:      "tinyint(1)",
				Collation: "NULL",
				Null:      "NO",
				Key:       "",
				Default:   "1",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "is_sys_disk_store",
				Type:      "tinyint unsigned",
				Collation: "NULL",
				Null:      "NO",
				Key:       "",
				Default:   "1",
			},
		},
		{
			info: SSqlColumnInfo{
				Field:     "is_sys_disk_store",
				Type:      "tinyint(3) unsigned",
				Collation: "NULL",
				Null:      "NO",
				Key:       "",
				Default:   "1",
			},
		},
	}
	for _, c := range cases {
		got := c.info.toColumnSpec()
		if got == nil {
			t.Errorf("fail to convert column spec")
		} else {
			t.Logf("column %s", got.DefinitionString())
		}
	}
}

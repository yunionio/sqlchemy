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

package dameng

import (
	"testing"

	"yunion.io/x/jsonutils"
)

func TestToColumnSpec(t *testing.T) {
	cases := []struct {
		info sSqlColumnInfo
	}{
		{
			info: sSqlColumnInfo{
				ColumnName:  "created_at",
				DataType:    "TIMESTAMP",
				Nullable:    "N",
				DataLength:  0,
				DataDefault: "NULL",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "updated_at",
				DataType:    "TIMESTAMP",
				Nullable:    "N",
				DataLength:  0,
				DataDefault: "NULL",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "update_version",
				DataType:    "INT",
				Nullable:    "N",
				DataLength:  4,
				DataDefault: "0",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "update_version",
				DataType:    "INT",
				Nullable:    "N",
				DataLength:  4,
				DataDefault: "0",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "update_version",
				DataType:    "INT",
				Nullable:    "N",
				DataLength:  4,
				DataDefault: "0",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "id",
				DataType:    "VARCHAR",
				Nullable:    "N",
				IsPrimary:   true,
				DataLength:  128,
				DataDefault: "NULL",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "name",
				DataType:    "VARCHAR",
				Nullable:    "N",
				IsPrimary:   true,
				DataLength:  128,
				DataDefault: "NULL",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "cmtbound",
				DataType:    "REAL",
				Nullable:    "Y",
				DataLength:  4,
				DataDefault: "1",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "is_sys_disk_store",
				DataType:    "TINYINT",
				Nullable:    "N",
				DataLength:  1,
				DataDefault: "1",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "is_sys_disk_store",
				DataType:    "TINYINT",
				Nullable:    "N",
				DataLength:  1,
				DataDefault: "1",
			},
		},
		{
			info: sSqlColumnInfo{
				ColumnName:  "is_sys_disk_store",
				DataType:    "TINYINT",
				Nullable:    "N",
				DataLength:  1,
				DataDefault: "1",
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

func TestDecodeInfo6(t *testing.T) {
	cases := []struct {
		name  string
		info6 []byte
		want  sDamengAutoIncrementInfo
	}{
		{
			name: "ROW_ID",
			info6: []byte{
				0x11, 0x5B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0xBA, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			want: sDamengAutoIncrementInfo{
				Name:   "row_id",
				Offset: 23313,
				Step:   1978,
				Dummy:  1,
			},
		},
	}
	for _, c := range cases {
		got, err := decodeInfo6(c.name, c.info6)
		if err != nil {
			t.Errorf("decodeInfo6 error %s", err)
		} else if jsonutils.Marshal(got).String() != jsonutils.Marshal(c.want).String() {
			t.Errorf("want: %s got %s", jsonutils.Marshal(c.want).String(), jsonutils.Marshal(got).String())
		}
	}
}

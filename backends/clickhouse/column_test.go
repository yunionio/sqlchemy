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
	"database/sql"
	"testing"

	"yunion.io/x/jsonutils"
	"yunion.io/x/pkg/tristate"
	"yunion.io/x/sqlchemy"
)

func TestBadColumns(t *testing.T) {
	wantPanic := func(t *testing.T, msgFmt string, msgVals ...interface{}) {
		if msg := recover(); msg == nil {
			t.Errorf(msgFmt, msgVals...)
		}
	}
	isPtr := false

	t.Run("bool default true", func(t *testing.T) {
		defer wantPanic(t, "non-pointer boolean must not have default value")
		bc := NewBooleanColumn(
			"bad_column",
			map[string]string{
				"default": "true",
			},
			isPtr,
		)
		def := bc.DefinitionString()
		if def != "" {
			t.Fatal("should have paniced")
		}
	})
	t.Run("Decimal missing width and precision", func(t *testing.T) {
		defer wantPanic(t, "ERROR 1101 (42000): BLOB/TEXT column 'xxx' can't have a default value")
		col := NewDecimalColumn(
			"bad",
			map[string]string{
				"default": "off",
			},
			isPtr,
		)
		def := col.DefinitionString()
		if def != "" {
			t.Fatal("should have paniced")
		}
	})
}

var (
	triCol         = NewTristateColumn("field", nil, false)
	notNullTriCol  = NewTristateColumn("field", nil, false)
	boolCol        = NewBooleanColumn("field", nil, false)
	notNullBoolCol = NewBooleanColumn("field", map[string]string{sqlchemy.TAG_NULLABLE: "false"}, false)
	intCol         = NewIntegerColumn("field", "Int8", nil, false)
	uIntCol        = NewIntegerColumn("field", "UInt32", nil, false)
	float32Col     = NewFloatColumn("field", "Float32", nil, false)
	float64Col     = NewFloatColumn("field", "Float64", nil, false)
	decimal32Col   = NewDecimalColumn("field", map[string]string{sqlchemy.TAG_WIDTH: "9", sqlchemy.TAG_PRECISION: "8"}, false)
	decimal64Col   = NewDecimalColumn("field", map[string]string{sqlchemy.TAG_WIDTH: "18", sqlchemy.TAG_PRECISION: "8"}, false)
	decimal128Col  = NewDecimalColumn("field", map[string]string{sqlchemy.TAG_WIDTH: "38", sqlchemy.TAG_PRECISION: "8"}, false)
	decimal256Col  = NewDecimalColumn("field", map[string]string{sqlchemy.TAG_WIDTH: "76", sqlchemy.TAG_PRECISION: "8"}, false)
	textCol        = NewTextColumn("field", "String", nil, false)
	charCol        = NewTextColumn("field", "String", map[string]string{sqlchemy.TAG_WIDTH: "16"}, false)
	notNullTextCol = NewTextColumn("field", "String", map[string]string{sqlchemy.TAG_WIDTH: "16", sqlchemy.TAG_NULLABLE: "false"}, false)
	defTextCol     = NewTextColumn("field", "String", map[string]string{sqlchemy.TAG_WIDTH: "16", sqlchemy.TAG_DEFAULT: "new!"}, false)
	dateCol        = NewDateTimeColumn("field", nil, false)
	ttlDateCol     = NewDateTimeColumn("field", map[string]string{TAG_TTL: "3m"}, false)
	notNullDateCol = NewDateTimeColumn("field", map[string]string{sqlchemy.TAG_NULLABLE: "false"}, false)
	compCol        = NewCompoundColumn("field", nil, false)
)

func TestColumns(t *testing.T) {
	cases := []struct {
		in   sqlchemy.IColumnSpec
		want string
	}{
		{
			in:   &triCol,
			want: "`field` Nullable(UInt8)",
		},
		{
			in:   &notNullTriCol,
			want: "`field` Nullable(UInt8)",
		},
		{
			in:   &boolCol,
			want: "`field` Nullable(UInt8)",
		},
		{
			in:   &notNullBoolCol,
			want: "`field` UInt8",
		},
		{
			in:   &intCol,
			want: "`field` Nullable(Int8)",
		},
		{
			in:   &uIntCol,
			want: "`field` Nullable(UInt32)",
		},
		{
			in:   &float32Col,
			want: "`field` Nullable(Float32)",
		},
		{
			in:   &float64Col,
			want: "`field` Nullable(Float64)",
		},
		{
			in:   &decimal32Col,
			want: "`field` Nullable(Decimal32(9, 8))",
		},
		{
			in:   &decimal64Col,
			want: "`field` Nullable(Decimal64(18, 8))",
		},
		{
			in:   &decimal128Col,
			want: "`field` Nullable(Decimal128(38, 8))",
		},
		{
			in:   &decimal256Col,
			want: "`field` Nullable(Decimal256(76, 8))",
		},
		{
			in:   &textCol,
			want: "`field` Nullable(String)",
		},
		{
			in:   &charCol,
			want: "`field` Nullable(String)",
		},
		{
			in:   &notNullTextCol,
			want: "`field` String",
		},
		{
			in:   &defTextCol,
			want: "`field` Nullable(String) DEFAULT 'new!'",
		},
		{
			in:   &dateCol,
			want: "`field` Nullable(DateTime('UTC'))",
		},
		{
			in:   &ttlDateCol,
			want: "`field` Nullable(DateTime('UTC'))",
		},
		{
			in:   &notNullDateCol,
			want: "`field` DateTime('UTC')",
		},
		{
			in:   &compCol,
			want: "`field` Nullable(String)",
		},
	}
	for _, c := range cases {
		got := c.in.DefinitionString()
		if got != c.want {
			t.Errorf("got %s want %s", got, c.want)
		}
	}
}

func TestConvertValue(t *testing.T) {
	cases := []struct {
		in   interface{}
		want interface{}
		col  sqlchemy.IColumnSpec
	}{
		{
			in:   true,
			want: uint8(1),
			col:  &boolCol,
		},
		{
			in:   false,
			want: uint8(0),
			col:  &boolCol,
		},
		{
			in:   tristate.True,
			want: uint8(1),
			col:  &triCol,
		},
		{
			in:   tristate.False,
			want: uint8(0),
			col:  &triCol,
		},
		{
			in:   tristate.None,
			want: sql.NullInt32{},
			col:  &triCol,
		},
		{
			in:   23,
			want: 23,
			col:  &intCol,
		},
		{
			in:   jsonutils.NewDict(),
			want: `{}`,
			col:  &compCol,
		},
	}
	for _, c := range cases {
		got := c.col.ConvertFromValue(c.in)
		if got != c.want {
			t.Errorf("%s [%#v] want: %#v got: %#v", c.col.DefinitionString(), c.in, c.want, got)
		}
	}
}
func TestConvertString(t *testing.T) {
	cases := []struct {
		in   string
		want interface{}
		col  sqlchemy.IColumnSpec
	}{
		{
			in:   `true`,
			want: uint8(1),
			col:  &boolCol,
		},
		{
			in:   "false",
			want: uint8(0),
			col:  &boolCol,
		},
		{
			in:   "true",
			want: uint8(1),
			col:  &triCol,
		},
		{
			in:   "false",
			want: uint8(0),
			col:  &triCol,
		},
		{
			in:   "none",
			want: sql.NullInt32{},
			col:  &triCol,
		},
		{
			in:   "23",
			want: int8(23),
			col:  &intCol,
		},
		{
			in:   "0.01",
			want: float32(0.01),
			col:  &float32Col,
		},
	}
	for _, c := range cases {
		got := c.col.ConvertFromString(c.in)
		if got != c.want {
			t.Errorf("%s [%s] want: %#v got: %#v", c.col.DefinitionString(), c.in, c.want, got)
		}
	}
}

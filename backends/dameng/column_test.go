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
		NewBooleanColumn(
			"bad_column",
			map[string]string{
				"default": "1",
			},
			isPtr,
		)
	})
}

var (
	triCol         = NewTristateColumn("", "field", nil, false)
	notNullTriCol  = NewTristateColumn("", "field", nil, false)
	boolCol        = NewBooleanColumn("field", nil, false)
	notNullBoolCol = NewBooleanColumn("field", map[string]string{sqlchemy.TAG_NULLABLE: "false"}, false)
	intCol         = NewIntegerColumn("field", "INT", nil, false)
	floatCol       = NewFloatColumn("field", "FLOAT", nil, false)
	textCol        = NewTextColumn("field", "TEXT", nil, false)
	charCol        = NewTextColumn("field", "VARCHAR", map[string]string{sqlchemy.TAG_WIDTH: "16"}, false)
	notNullTextCol = NewTextColumn("field", "VARCHAR", map[string]string{sqlchemy.TAG_WIDTH: "16", sqlchemy.TAG_NULLABLE: "false"}, false)
	defTextCol     = NewTextColumn("field", "VARCHAR", map[string]string{sqlchemy.TAG_WIDTH: "16", sqlchemy.TAG_DEFAULT: "new!"}, false)
	dateCol        = NewDateTimeColumn("field", nil, false)
	notNullDateCol = NewDateTimeColumn("field", map[string]string{sqlchemy.TAG_NULLABLE: "false"}, false)
	compCol        = NewCompoundColumn("field", "TEXT", nil, false)
)

func TestColumns(t *testing.T) {
	cases := []struct {
		in   sqlchemy.IColumnSpec
		want string
	}{
		{
			in:   &triCol,
			want: `"field" TINYINT`,
		},
		{
			in:   &notNullTriCol,
			want: `"field" TINYINT`,
		},
		{
			in:   &boolCol,
			want: `"field" TINYINT`,
		},
		{
			in:   &notNullBoolCol,
			want: `"field" TINYINT DEFAULT 0 NOT NULL`,
		},
		{
			in:   &intCol,
			want: `"field" INT`,
		},
		{
			in:   &floatCol,
			want: `"field" FLOAT`,
		},
		{
			in:   &textCol,
			want: `"field" TEXT`,
		},
		{
			in:   &charCol,
			want: `"field" VARCHAR(16)`,
		},
		{
			in:   &notNullTextCol,
			want: `"field" VARCHAR(16) DEFAULT '' NOT NULL`,
		},
		{
			in:   &defTextCol,
			want: `"field" VARCHAR(16) DEFAULT 'new!'`,
		},
		{
			in:   &dateCol,
			want: `"field" TIMESTAMP(0)`,
		},
		{
			in:   &notNullDateCol,
			want: `"field" TIMESTAMP(0) NOT NULL`,
		},
		{
			in:   &compCol,
			want: `"field" TEXT`,
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
			want: 1,
			col:  &boolCol,
		},
		{
			in:   false,
			want: 0,
			col:  &boolCol,
		},
		{
			in:   tristate.True,
			want: 1,
			col:  &triCol,
		},
		{
			in:   tristate.False,
			want: 0,
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
			want: 1,
			col:  &boolCol,
		},
		{
			in:   "false",
			want: 0,
			col:  &boolCol,
		},
		{
			in:   "true",
			want: 1,
			col:  &triCol,
		},
		{
			in:   "false",
			want: 0,
			col:  &triCol,
		},
		{
			in:   "none",
			want: sql.NullInt32{},
			col:  &triCol,
		},
		{
			in:   "23",
			want: int64(23),
			col:  &intCol,
		},
		{
			in:   "0.01",
			want: 0.01,
			col:  &floatCol,
		},
	}
	for _, c := range cases {
		got := c.col.ConvertFromString(c.in)
		if got != c.want {
			t.Errorf("%s [%s] want: %#v got: %#v", c.col.DefinitionString(), c.in, c.want, got)
		}
	}
}

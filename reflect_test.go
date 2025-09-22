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
	"reflect"
	"testing"
	"time"

	"yunion.io/x/jsonutils"
	"yunion.io/x/pkg/gotypes"
	"yunion.io/x/pkg/tristate"
	"yunion.io/x/pkg/util/reflectutils"
	"yunion.io/x/pkg/util/timeutils"
)

type SerializableType struct {
	I  int
	S  string
	IS []int
	SS []string
	M  map[string]int
}

func (t *SerializableType) IsZero() bool {
	return reflect.DeepEqual(t, &SerializableType{})
}

func (t *SerializableType) String() string {
	return jsonutils.Marshal(t).String()
}

func Test_setValueBySQLString(t *testing.T) {
	t.Run("serializable", func(t *testing.T) {
		gotypes.RegisterSerializable(reflect.TypeOf((*SerializableType)(nil)), func() gotypes.ISerializable {
			return &SerializableType{}
		})

		v := &SerializableType{
			I:  100,
			S:  "serializable s value",
			IS: []int{200, 201},
			SS: []string{"s0", "s 1", "s 2"},
			M: map[string]int{
				"k0":  0,
				"k 1": 2,
			},
		}
		s := v.String()

		vv := &SerializableType{}
		vvv := reflect.ValueOf(&vv).Elem()
		if !vvv.CanAddr() {
			t.Fatalf("can not addr")
		}
		if err := setValueBySQLString(vvv, s); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(v, vv) {
			t.Fatalf("unequal, want:\n%s\ngot:\n%s", v, vv)
		}
	})

	t.Run("JSONObject", func(t *testing.T) {
		var (
			v     jsonutils.JSONObject
			wantV jsonutils.JSONObject
			s     = `{"i":100,"is":[200,201],"m":{"k 1":2,"k0":0},"s":"serializable s value","ss":["s0","s 1","s 2"]}`
			err   error
		)

		if wantV, err = jsonutils.ParseString(s); err != nil {
			t.Fatalf("parse test json string: %v", err)
		}
		vv := reflect.ValueOf(&v).Elem()
		if err := setValueBySQLString(vv, s); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(wantV, v) {
			t.Fatalf("unequal, want:\n%s\ngot:\n%s", v, vv)
		}
	})
}

func TestGetQuoteStringValue(t *testing.T) {
	cases := []struct {
		in   interface{}
		want string
	}{
		{
			in:   0,
			want: "0",
		},
		{
			in:   "abc",
			want: "'abc'",
		},
		{
			in:   "123\"34",
			want: "'123\"34'",
		},
		{
			in:   "123'34",
			want: "'123\\'34'",
		},
	}
	for _, c := range cases {
		got := getQuoteStringValue(c.in)
		if got != c.want {
			t.Errorf("want %s got %s for %s", c.want, got, c.in)
		}
	}
}

type STag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type STestStruct struct {
	IntV int `json:"int_v"`

	UIntV uint `json:"uint_v"`

	BoolV *bool `json:"bool_v"`

	BoolV2 bool `json:"bool_v2"`

	FloatV float64 `json:"float_v"`

	StrV string `json:"str_v"`

	TristateV tristate.TriState `json:"tristate_v"`

	IntA []int `json:"int_a"`

	TagA []STag `json:"tag_a"`

	MapA map[string]string `json:"map_a"`

	TimeV time.Time `json:"time_v"`
}

var (
	ts = &STestStruct{}
)

func TestSetValueBySQLString(t *testing.T) {
	tsValue := reflect.Indirect(reflect.ValueOf(ts))
	ss := reflectutils.FetchAllStructFieldValueSetForWrite(tsValue)

	cases := []struct {
		field  string
		sqlstr string
		want   interface{}
	}{
		{
			field:  "int_v",
			sqlstr: "1",
			want:   1,
		},
		{
			field:  "uint_v",
			sqlstr: "1",
			want:   uint(1),
		},
		{
			field:  "bool_v",
			sqlstr: "1",
			want: func() *bool {
				v := true
				return &v
			}(),
		},
		{
			field:  "bool_v2",
			sqlstr: "2",
			want:   true,
		},
		{
			field:  "bool_v2",
			sqlstr: "0",
			want:   false,
		},
		{
			field:  "float_v",
			sqlstr: "1.234",
			want:   float64(1.234),
		},
		{
			field:  "tristate_v",
			sqlstr: "0",
			want:   tristate.False,
		},
		{
			field:  "tristate_v",
			sqlstr: "1",
			want:   tristate.True,
		},
		{
			field:  "tristate_v",
			sqlstr: "2",
			want:   tristate.None,
		},
		{
			field:  "tristate_v",
			sqlstr: "none",
			want:   tristate.None,
		},
		{
			field:  "str_v",
			sqlstr: "abcdEF",
			want:   "abcdEF",
		},
		{
			field:  "int_a",
			sqlstr: "[1,3,5,7,9]",
			want:   []int{1, 3, 5, 7, 9},
		},
		{
			field:  "tag_a",
			sqlstr: `[{"key":"name","value":"John"},{"key":"gender","value":"male"}]`,
			want: []STag{
				{
					Key:   "name",
					Value: "John",
				},
				{
					Key:   "gender",
					Value: "male",
				},
			},
		},
		{
			field:  "map_a",
			sqlstr: `{"name":"John","gender":"male"}`,
			want: map[string]string{
				"name":   "John",
				"gender": "male",
			},
		},
		{
			field:  "time_v",
			sqlstr: "2021-10-01T00:00:00Z",
			want: func() time.Time {
				tm, _ := timeutils.ParseTimeStr("2021-10-01T00:00:00Z")
				return tm
			}(),
		},
	}
	for _, c := range cases {
		v, ok := ss.GetValue(c.field)
		if !ok {
			t.Errorf("GetValue %s not exist", c.field)
		} else {
			err := setValueBySQLString(v, c.sqlstr)
			if err != nil {
				t.Errorf("setValueBySQLString %s %s", c.field, err)
			} else {
				if !reflect.DeepEqual(v.Interface(), c.want) {
					t.Errorf("str: %s got: %v want: %v", c.sqlstr, v, c.want)
				}
			}
		}
	}
}

func TestConvertValueToTime(t *testing.T) {
	cases := []struct {
		in   interface{}
		want time.Time
	}{
		{
			in: "2021-10-01T00:00:00Z",
			want: func() time.Time {
				tm, _ := timeutils.ParseTimeStr("2021-10-01T00:00:00Z")
				return tm
			}(),
		},
	}
	for _, c := range cases {
		got := ConvertValueToTime(c.in)
		if got != c.want {
			t.Errorf("want %s got %s for %s", c.want, got, c.in)
		}
	}
}

func TestConvertValueToBool(t *testing.T) {
	cases := []struct {
		in   interface{}
		want bool
	}{
		{
			in:   "0",
			want: false,
		},
		{
			in:   "1",
			want: true,
		},
		{
			in:   tristate.True,
			want: true,
		},
		{
			in:   tristate.False,
			want: false,
		},
		{
			in:   tristate.None,
			want: false,
		},
	}
	for _, c := range cases {
		got := ConvertValueToBool(c.in)
		if got != c.want {
			t.Errorf("want %v got %v for %s", c.want, got, c.in)
		}
	}
}

func TestConvertValueToTriState(t *testing.T) {
	cases := []struct {
		in   interface{}
		want tristate.TriState
	}{
		{
			in:   "0",
			want: tristate.False,
		},
		{
			in:   "1",
			want: tristate.True,
		},
		{
			in:   "2",
			want: tristate.False,
		},
		{
			in:   "none",
			want: tristate.None,
		},
		{
			in:   "true",
			want: tristate.True,
		},
		{
			in:   "false",
			want: tristate.False,
		},
		{
			in:   tristate.False,
			want: tristate.False,
		},
		{
			in:   tristate.True,
			want: tristate.True,
		},
		{
			in:   tristate.None,
			want: tristate.None,
		},
		{
			in:   nil,
			want: tristate.None,
		},
		{
			in:   1,
			want: tristate.True,
		},
		{
			in:   0,
			want: tristate.False,
		},
		{
			in:   uint8(1),
			want: tristate.True,
		},
		{
			in:   uint8(0),
			want: tristate.False,
		},
	}
	for _, c := range cases {
		got := ConvertValueToTriState(c.in)
		if got != c.want {
			t.Errorf("want %s got %s for %s", c.want, got, c.in)
		}
	}
}

func TestEqualsGrossValue(t *testing.T) {
	cases := []struct {
		of   interface{}
		nf   interface{}
		want bool
	}{
		{
			of:   1.0,
			nf:   1.0,
			want: true,
		},
		{
			of:   1.0,
			nf:   1.00001,
			want: false,
		},
		{
			of:   1.0,
			nf:   0.99999,
			want: false,
		},
		{
			of:   1.0,
			nf:   1.000002,
			want: false,
		},
		{
			of:   1.0,
			nf:   0.999998,
			want: false,
		},
		{
			of:   1.0,
			nf:   1.000001,
			want: false,
		},
		{
			of:   1.0,
			nf:   1.0000011,
			want: false,
		},
		{
			of:   1.0,
			nf:   1.0000009,
			want: true,
		},
		{
			of:   1.0,
			nf:   0.999999,
			want: false,
		},
		{
			of:   1.0,
			nf:   0.9999989,
			want: false,
		},
		{
			of:   1.0,
			nf:   0.9999991,
			want: true,
		},
		{
			of:   1.0,
			nf:   1.0000001,
			want: true,
		},
		{
			of:   1.0,
			nf:   0.9999999,
			want: true,
		},
		{
			of:   "1.0",
			nf:   "0.9999999",
			want: true,
		},
		{
			of:   1,
			nf:   "0.9999999",
			want: true,
		},
		{
			of:   "2025-01-01T00:00:00.000000Z",
			nf:   "2025-01-01T00:00:00.000001Z",
			want: true,
		},
		{
			of:   "2025-01-01T00:00:00.000000Z",
			nf:   "2025-01-01T00:00:00.001001Z",
			want: true,
		},
		{
			of:   "2025-01-01T00:00:00.000000Z",
			nf:   "2025-01-01T00:00:01.001000Z",
			want: false,
		},
		{
			of:   "2025-01-01T00:00:00.999999Z",
			nf:   "2025-01-01T00:00:01.000000Z",
			want: true,
		},
		{
			of:   "2025-01-01T00:00:00.999999Z",
			nf:   "2025-01-01T00:00:01.999000Z",
			want: true,
		},
		{
			of:   "2025-01-01T00:00:00.999999Z",
			nf:   "2025-01-01T00:00:01.999999Z",
			want: false,
		},
	}
	for _, c := range cases {
		got := EqualsGrossValue(c.of, c.nf)
		if got != c.want {
			t.Errorf("want %v got %v for %v and %v", c.want, got, c.of, c.nf)
		}
	}
}

package sqlchemy

import (
	"reflect"
	"testing"

	"yunion.io/x/jsonutils"
)

func TestBaseColumns(t *testing.T) {
	cases := []struct {
		name      string
		sqlType   string
		tags      map[string]string
		isPointer bool
		want      SBaseColumn
	}{
		{
			name:      "test",
			sqlType:   "TEXT",
			tags:      map[string]string{},
			isPointer: false,
			want: SBaseColumn{
				name:       "test",
				dbName:     "",
				sqlType:    "TEXT",
				isPointer:  false,
				isNullable: true,
				isPrimary:  false,
				tags:       make(map[string]string),
				colIndex:   -1,
			},
		},
		{
			name:      "test",
			sqlType:   "TEXT",
			tags:      map[string]string{"primary": "true"},
			isPointer: false,
			want: SBaseColumn{
				name:       "test",
				dbName:     "",
				sqlType:    "TEXT",
				isPointer:  false,
				isNullable: false,
				isPrimary:  true,
				tags:       make(map[string]string),
				colIndex:   -1,
			},
		},
		{
			name:      "test",
			sqlType:   "TEXT",
			tags:      map[string]string{"primary": "true", "index": "true"},
			isPointer: false,
			want: SBaseColumn{
				name:       "test",
				dbName:     "",
				sqlType:    "TEXT",
				isPointer:  false,
				isNullable: false,
				isPrimary:  true,
				isIndex:    true,
				tags:       make(map[string]string),
				colIndex:   -1,
			},
		},
		{
			name:      "test",
			sqlType:   "TEXT",
			tags:      map[string]string{"primary": "true", "index": "true", "name": "test_"},
			isPointer: false,
			want: SBaseColumn{
				name:       "test",
				dbName:     "test_",
				sqlType:    "TEXT",
				isPointer:  false,
				isNullable: false,
				isPrimary:  true,
				isIndex:    true,
				tags:       make(map[string]string),
				colIndex:   -1,
			},
		},
		{
			name:      "test",
			sqlType:   "TEXT",
			tags:      map[string]string{"primary": "true", "index": "true", "name": "test_", "sql_name": "test2_"},
			isPointer: false,
			want: SBaseColumn{
				name:       "test",
				dbName:     "test2_",
				sqlType:    "TEXT",
				isPointer:  false,
				isNullable: false,
				isPrimary:  true,
				isIndex:    true,
				tags:       make(map[string]string),
				colIndex:   -1,
			},
		},
		{
			name:      "test",
			sqlType:   "TEXT",
			tags:      map[string]string{"primary": "true", "index": "true", "sql_name": "test2_"},
			isPointer: false,
			want: SBaseColumn{
				name:       "test",
				dbName:     "test2_",
				sqlType:    "TEXT",
				isPointer:  false,
				isNullable: false,
				isPrimary:  true,
				isIndex:    true,
				tags:       make(map[string]string),
				colIndex:   -1,
			},
		},
	}
	for _, c := range cases {
		got := NewBaseColumn(c.name, c.sqlType, c.tags, c.isPointer)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("want: %#v got: %#v", c.want, got)
		}
	}
}

func TestConvertFromString(t *testing.T) {
	cases := []struct {
		in   string
		want interface{}
	}{
		{
			in:   `{"name":"John"}`,
			want: `{"name":"John"}`,
		},
		{
			in:   "test",
			want: `"test"`,
		},
		{
			in:   "",
			want: "null",
		},
	}
	for _, c := range cases {
		cc := SBaseCompoundColumn{}
		got := cc.ConvertFromString(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("want: %s got %s", jsonutils.Marshal(c.want), jsonutils.Marshal(got))
		}
	}
}

type sSerial struct {
}

func (s *sSerial) String() string {
	return "test"
}

func (s *sSerial) IsZero() bool {
	return false
}

func TestConvertFromValue(t *testing.T) {
	cases := []struct {
		in   interface{}
		want interface{}
	}{
		{
			in:   &sSerial{},
			want: `test`,
		},
		{
			in: struct {
				Name string
			}{
				Name: "abc",
			},
			want: `{"name":"abc"}`,
		},
	}
	for _, c := range cases {
		cc := SBaseCompoundColumn{}
		got := cc.ConvertFromValue(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("want: %s got %s", jsonutils.Marshal(c.want), jsonutils.Marshal(got))
		}
	}
}

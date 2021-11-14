package sqlchemy

import (
	"reflect"
	"testing"
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

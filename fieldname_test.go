package sqlchemy

import (
	"testing"

	"yunion.io/x/jsonutils"
)

func TestGetColumnName(t *testing.T) {
	type SEmbed struct {
		Id   string `name:"id"`
		Desc string `name:"description"`
	}
	type STestStruct struct {
		Name       string              `name:"name"`
		ProjectId  string              `name:"tenant_id"`
		Age        int                 `name:"age"`
		Gender     bool                `name:"gender"`
		Properties *jsonutils.JSONDict `name:"properties"`

		SEmbed
	}
	s := STestStruct{}
	cases := []struct {
		field interface{}
		want  string
	}{
		{
			field: &s.Id,
			want:  "id",
		},
		{
			field: &s.Desc,
			want:  "description",
		},
		{
			field: &s.Name,
			want:  "name",
		},
		{
			field: &s.ProjectId,
			want:  "tenant_id",
		},
		{
			field: &s.Age,
			want:  "age",
		},
		{
			field: &s.Gender,
			want:  "gender",
		},
		{
			field: &s.Properties,
			want:  "properties",
		},
	}
	for _, c := range cases {
		got := GetColumnName(&s, c.field)
		if got != c.want {
			t.Errorf("want: %s got: %s", c.want, got)
		}
	}
	RegisterStructFieldNames(&s)
	for _, c := range cases {
		got := Fn(c.field)
		if got != c.want {
			t.Errorf("want: %s got: %s", c.want, got)
		}
	}
}

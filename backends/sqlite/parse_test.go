package sqlite

import "testing"

func TestParseTableIndex(t *testing.T) {
	cases := []struct {
		in   string
		name string
		want []string
	}{
		{
			in:   "CREATE INDEX `ix_testtable_name` ON `testtable`(`name`)",
			name: "ix_testtable_name",
			want: []string{"name"},
		},
		{
			in:   "CREATE INDEX `ix_testtable_name` ON `testtable`(`name`, `type`)",
			name: "ix_testtable_name",
			want: []string{"name", "type"},
		},
	}
	for _, c := range cases {
		ti := sSqliteTableInfo{
			Type: "index",
			Name: "index",
			Sql:  c.in,
		}
		index, err := ti.parseTableIndex(nil)
		if err != nil {
			t.Errorf("parseTableIndex fail %s", err)
		} else {
			if index.Name() != c.name {
				t.Errorf("want name: %s != got %s", c.name, index.Name())
			} else if !index.IsIdentical(c.want...) {
				t.Errorf("want: %s != got: %s", c.want, index.QuotedColumns("`"))
			}
		}
	}
}

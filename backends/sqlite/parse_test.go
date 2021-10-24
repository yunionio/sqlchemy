package sqlite

import "testing"

func TestParseTableIndex(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{
			in:   "CREATE INDEX `ix_testtable_name` ON `testtable`(`name`)",
			want: []string{"name"},
		},
		{
			in:   "CREATE INDEX `ix_testtable_name` ON `testtable`(`name`, `type`)",
			want: []string{"name", "type"},
		},
	}
	for _, c := range cases {
		ti := sSqliteTableInfo{
			Type: "index",
			Name: "index",
			Sql:  c.in,
		}
		index, err := ti.parseTableIndex()
		if err != nil {
			t.Errorf("parseTableIndex fail %s", err)
		} else {
			if !index.IsIdentical(c.want...) {
				t.Errorf("want: %s != got: %s", c.want, index.QuotedColumns())
			}
		}
	}
}

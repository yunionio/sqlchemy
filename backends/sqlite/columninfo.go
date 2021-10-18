package sqlite

import (
	"yunion.io/x/sqlchemy"
)

type sSqlColumnInfo struct {
	Field      string
	Type       string
	Collation  string
	Null       string
	Key        string
	Default    string
	Extra      string
	Privileges string
	Comment    string
}

func (info *sSqlColumnInfo) toColumnSpec(table *sqlchemy.STableSpec) sqlchemy.IColumnSpec {
	return nil
}

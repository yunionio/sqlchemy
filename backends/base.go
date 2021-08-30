package backends

import (
	"yunion.io/x/sqlchemy"
)

type SBaseBackend struct {
}

func (bb *SBaseBackend) Name() sqlchemy.DBBackendName {
	return ""
}

func (bb *SBaseBackend) GetTableSQL() string {
	return "SHOW TABLES"
}

func (bb *SBaseBackend) GetCreateSQL(ts sqlchemy.ITableSpec) string {
	return ""
}

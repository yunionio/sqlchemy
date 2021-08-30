package clickhouse

import (
	_ "github.com/ClickHouse/clickhouse-go"

	"yunion.io/x/sqlchemy"
	"yunion.io/x/sqlchemy/backends"
)

func init() {
	sqlchemy.RegisterBackend(&SClickhouseBackend{})
}

type SClickhouseBackend struct {
	backends.SBaseBackend
}

func (click *SClickhouseBackend) Name() sqlchemy.DBBackendName {
	return sqlchemy.ClickhouseBackend
}

func (click *SClickhouseBackend) IsSupportIndexAndContraints() bool {
	return false
}

func (click *SClickhouseBackend) FetchTableColumnSpecs(ts sqlchemy.ITableSpec) ([]sqlchemy.IColumnSpec, error) {
	// XXX: TO DO
	return ts.Columns(), nil
}

package sqlite

import (
	"fmt"
	"reflect"

	_ "github.com/mattn/go-sqlite3"

	"yunion.io/x/pkg/gotypes"
	"yunion.io/x/pkg/tristate"
	"yunion.io/x/sqlchemy"
)

func init() {
	sqlchemy.RegisterBackend(&SSqliteBackend{})
}

type SSqliteBackend struct {
	sqlchemy.SBaseBackend
}

func (sqlite *SSqliteBackend) Name() sqlchemy.DBBackendName {
	return sqlchemy.SQLiteBackend
}

func (sqlite *SSqliteBackend) GetTableSQL() string {
	return "SELECT name FROM sqlite_master WHERE type='table'"
}

func (sqlite *SSqliteBackend) IsSupportIndexAndContraints() bool {
	return false
}

func (sqlite *SSqliteBackend) FetchTableColumnSpecs(ts sqlchemy.ITableSpec) ([]sqlchemy.IColumnSpec, error) {
	sql := fmt.Sprintf("PRAGMA table_info(`%s`);", ts.Name())
	query := ts.Database().NewRawQuery(sql, "field", "type", "collation", "null", "key", "default", "extra", "privileges", "comment")
	infos := make([]sSqlColumnInfo, 0)
	err := query.All(&infos)
	if err != nil {
		return nil, err
	}
	specs := make([]sqlchemy.IColumnSpec, 0)
	for _, info := range infos {
		specs = append(specs, info.toColumnSpec(ts.(*sqlchemy.STableSpec)))
	}
	return specs, nil
}

func (sqlite *SSqliteBackend) GetColumnSpecByFieldType(table *sqlchemy.STableSpec, fieldType reflect.Type, fieldname string, tagmap map[string]string, isPointer bool) sqlchemy.IColumnSpec {
	switch fieldType {
	case tristate.TriStateType:
		col := table.NewTristateColumn(fieldname, "INTEGER", tagmap, isPointer)
		return &col
	case gotypes.TimeType:
		col := table.NewDateTimeColumn(fieldname, "TEXT", tagmap, isPointer)
		return &col
	}
	switch fieldType.Kind() {
	case reflect.String:
		col := table.NewTextColumn(fieldname, "TEXT", tagmap, isPointer)
		return &col
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		col := table.NewIntegerColumn(fieldname, "INTEGER", false, tagmap, isPointer)
		return &col
	case reflect.Bool:
		col := table.NewBooleanColumn(fieldname, "INTEGER", tagmap, isPointer)
		return &col
	case reflect.Float32, reflect.Float64:
		col := table.NewFloatColumn(fieldname, "REAL", tagmap, isPointer)
		return &col
	}
	if fieldType.Implements(gotypes.ISerializableType) {
		col := table.NewCompoundColumn(fieldname, "TEXT", tagmap, isPointer)
		return &col
	}
	return nil
}

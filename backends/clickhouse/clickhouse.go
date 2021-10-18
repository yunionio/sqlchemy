package clickhouse

import (
	"bytes"
	"fmt"
	"reflect"

	_ "github.com/ClickHouse/clickhouse-go"

	"yunion.io/x/pkg/gotypes"
	"yunion.io/x/pkg/tristate"
	"yunion.io/x/sqlchemy"
)

func init() {
	sqlchemy.RegisterBackend(&SClickhouseBackend{})
}

type SClickhouseBackend struct {
	sqlchemy.SBaseBackend
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

func (click *SClickhouseBackend) GetColumnSpecByFieldType(table *sqlchemy.STableSpec, fieldType reflect.Type, fieldname string, tagmap map[string]string, isPointer bool) sqlchemy.IColumnSpec {
	switch fieldType {
	case tristate.TriStateType:
		col := table.NewTristateColumn(fieldname, "Boolean", tagmap, isPointer)
		return &col
	case gotypes.TimeType:
		col := table.NewDateTimeColumn(fieldname, "DateTime", tagmap, isPointer)
		return &col
	}
	switch fieldType.Kind() {
	case reflect.String:
		col := table.NewTextColumn(fieldname, "String", tagmap, isPointer)
		return &col
	case reflect.Int, reflect.Int32:
		col := table.NewIntegerColumn(fieldname, "Int32", false, tagmap, isPointer)
		return &col
	case reflect.Int8:
		col := table.NewIntegerColumn(fieldname, "Int8", false, tagmap, isPointer)
		return &col
	case reflect.Int16:
		col := table.NewIntegerColumn(fieldname, "Int16", false, tagmap, isPointer)
		return &col
	case reflect.Int64:
		col := table.NewIntegerColumn(fieldname, "Int64", false, tagmap, isPointer)
		return &col
	case reflect.Uint, reflect.Uint32:
		col := table.NewIntegerColumn(fieldname, "Int32", true, tagmap, isPointer)
		return &col
	case reflect.Uint8:
		col := table.NewIntegerColumn(fieldname, "Int8", true, tagmap, isPointer)
		return &col
	case reflect.Uint16:
		col := table.NewIntegerColumn(fieldname, "Int16", true, tagmap, isPointer)
		return &col
	case reflect.Uint64:
		col := table.NewIntegerColumn(fieldname, "Int64", true, tagmap, isPointer)
		return &col
	case reflect.Bool:
		col := table.NewBooleanColumn(fieldname, "Boolean", tagmap, isPointer)
		return &col
	case reflect.Float32:
		if _, ok := tagmap[sqlchemy.TAG_WIDTH]; ok {
			col := table.NewDecimalColumn(fieldname, "Decimal", tagmap, isPointer)
			return &col
		}
		col := table.NewFloatColumn(fieldname, "Float32", tagmap, isPointer)
		return &col
	case reflect.Float64:
		if _, ok := tagmap[sqlchemy.TAG_WIDTH]; ok {
			col := table.NewDecimalColumn(fieldname, "Decimal", tagmap, isPointer)
			return &col
		}
		col := table.NewFloatColumn(fieldname, "Float64", tagmap, isPointer)
		return &col
	}
	if fieldType.Implements(gotypes.ISerializableType) {
		col := table.NewCompoundColumn(fieldname, "String", tagmap, isPointer)
		return &col
	}
	return nil
}

func (click *SClickhouseBackend) ColumnDefinitionBuffer(c sqlchemy.IColumnSpec) bytes.Buffer {
	var buf bytes.Buffer

	buf.WriteByte('`')
	buf.WriteString(c.Name())
	buf.WriteByte('`')
	buf.WriteByte(' ')

	if c.IsNullable() {
		buf.WriteString("Nullable(")
	}

	buf.WriteString(c.ColType())

	if c.IsNullable() {
		buf.WriteString(")")
	}

	def := c.Default()
	defOk := c.IsSupportDefault()
	if def != "" {
		if !defOk {
			panic(fmt.Errorf("column %q type %q does not support having default value: %q",
				c.Name(), c.ColType(), def,
			))
		}
		def = c.ConvertFromString(def)
		buf.WriteString(" DEFAULT ")
		if c.IsText() {
			buf.WriteByte('\'')
		}
		buf.WriteString(def)
		if c.IsText() {
			buf.WriteByte('\'')
		}
	}

	return buf
}

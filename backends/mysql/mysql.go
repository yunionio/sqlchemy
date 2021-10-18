package mysql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"yunion.io/x/pkg/gotypes"
	"yunion.io/x/pkg/tristate"
	"yunion.io/x/pkg/util/regutils"
	"yunion.io/x/sqlchemy"
)

func init() {
	sqlchemy.RegisterBackend(&SMySQLBackend{})
}

type SMySQLBackend struct {
	sqlchemy.SBaseBackend
}

func (mysql *SMySQLBackend) Name() sqlchemy.DBBackendName {
	return sqlchemy.MySQLBackend
}

func (mysql *SMySQLBackend) GetCreateSQL(ts sqlchemy.ITableSpec) string {
	cols := make([]string, 0)
	primaries := make([]string, 0)
	indexes := make([]string, 0)
	autoInc := ""
	for _, c := range ts.Columns() {
		cols = append(cols, c.DefinitionString())
		if c.IsPrimary() {
			primaries = append(primaries, fmt.Sprintf("`%s`", c.Name()))
			if intC, ok := c.(*sqlchemy.SIntegerColumn); ok && intC.AutoIncrementOffset > 0 {
				autoInc = fmt.Sprintf(" AUTO_INCREMENT=%d", intC.AutoIncrementOffset)
			}
		}
		if c.IsIndex() {
			indexes = append(indexes, fmt.Sprintf("KEY `ix_%s_%s` (`%s`)", ts.Name(), c.Name(), c.Name()))
		}
	}
	if len(primaries) > 0 {
		cols = append(cols, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaries, ", ")))
	}
	if len(indexes) > 0 {
		cols = append(cols, indexes...)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci%s", ts.Name(), strings.Join(cols, ",\n"), autoInc)
}

func (msyql *SMySQLBackend) IsSupportIndexAndContraints() bool {
	return true
}

func (mysql *SMySQLBackend) FetchTableColumnSpecs(ts sqlchemy.ITableSpec) ([]sqlchemy.IColumnSpec, error) {
	sql := fmt.Sprintf("SHOW FULL COLUMNS IN `%s`", ts.Name())
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

func getTextSqlType(tagmap map[string]string) string {
	var width int
	var sqltype string
	widthStr, _ := tagmap[sqlchemy.TAG_WIDTH]
	if len(widthStr) > 0 && regutils.MatchInteger(widthStr) {
		width, _ = strconv.Atoi(widthStr)
	}
	txtLen, _ := tagmap[sqlchemy.TAG_TEXT_LENGTH]
	if width == 0 {
		switch strings.ToLower(txtLen) {
		case "medium":
			sqltype = "MEDIUMTEXT"
		case "long":
			sqltype = "LONGTEXT"
		default:
			sqltype = "TEXT"
		}
	} else {
		sqltype = "VARCHAR"
	}
	return sqltype
}

func (mysql *SMySQLBackend) GetColumnSpecByFieldType(table *sqlchemy.STableSpec, fieldType reflect.Type, fieldname string, tagmap map[string]string, isPointer bool) sqlchemy.IColumnSpec {
	switch fieldType {
	case tristate.TriStateType:
		tagmap[sqlchemy.TAG_WIDTH] = "1"
		col := table.NewTristateColumn(fieldname, "TINYINT", tagmap, isPointer)
		return &col
	case gotypes.TimeType:
		col := table.NewDateTimeColumn(fieldname, "DATETIME", tagmap, isPointer)
		return &col
	}
	switch fieldType.Kind() {
	case reflect.String:
		col := table.NewTextColumn(fieldname, getTextSqlType(tagmap), tagmap, isPointer)
		return &col
	case reflect.Int, reflect.Int32:
		tagmap[sqlchemy.TAG_WIDTH] = intWidthString("INT")
		col := table.NewIntegerColumn(fieldname, "INT", false, tagmap, isPointer)
		return &col
	case reflect.Int8:
		tagmap[sqlchemy.TAG_WIDTH] = intWidthString("TINYINT")
		col := table.NewIntegerColumn(fieldname, "TINYINT", false, tagmap, isPointer)
		return &col
	case reflect.Int16:
		tagmap[sqlchemy.TAG_WIDTH] = intWidthString("SMALLINT")
		col := table.NewIntegerColumn(fieldname, "SMALLINT", false, tagmap, isPointer)
		return &col
	case reflect.Int64:
		tagmap[sqlchemy.TAG_WIDTH] = intWidthString("BIGINT")
		col := table.NewIntegerColumn(fieldname, "BIGINT", false, tagmap, isPointer)
		return &col
	case reflect.Uint, reflect.Uint32:
		tagmap[sqlchemy.TAG_WIDTH] = uintWidthString("INT")
		col := table.NewIntegerColumn(fieldname, "INT", true, tagmap, isPointer)
		return &col
	case reflect.Uint8:
		tagmap[sqlchemy.TAG_WIDTH] = uintWidthString("TINYINT")
		col := table.NewIntegerColumn(fieldname, "TINYINT", true, tagmap, isPointer)
		return &col
	case reflect.Uint16:
		tagmap[sqlchemy.TAG_WIDTH] = uintWidthString("SMALLINT")
		col := table.NewIntegerColumn(fieldname, "SMALLINT", true, tagmap, isPointer)
		return &col
	case reflect.Uint64:
		tagmap[sqlchemy.TAG_WIDTH] = uintWidthString("BIGINT")
		col := table.NewIntegerColumn(fieldname, "BIGINT", true, tagmap, isPointer)
		return &col
	case reflect.Bool:
		tagmap[sqlchemy.TAG_WIDTH] = "1"
		col := table.NewBooleanColumn(fieldname, "TINYINT", tagmap, isPointer)
		return &col
	case reflect.Float32, reflect.Float64:
		if _, ok := tagmap[sqlchemy.TAG_WIDTH]; ok {
			col := table.NewDecimalColumn(fieldname, "DECIMAL", tagmap, isPointer)
			return &col
		}
		colType := "FLOAT"
		if fieldType == gotypes.Float64Type {
			colType = "DOUBLE"
		}
		col := table.NewFloatColumn(fieldname, colType, tagmap, isPointer)
		return &col
	}
	if fieldType.Implements(gotypes.ISerializableType) {
		col := table.NewCompoundColumn(fieldname, getTextSqlType(tagmap), tagmap, isPointer)
		return &col
	}
	return nil
}

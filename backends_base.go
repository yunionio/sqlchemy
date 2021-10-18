package sqlchemy

import (
	"bytes"
	"fmt"
)

type SBaseBackend struct {
}

func (bb *SBaseBackend) GetTableSQL() string {
	return "SHOW TABLES"
}

func (bb *SBaseBackend) GetCreateSQL(ts ITableSpec) string {
	return ""
}

func (bb *SBaseBackend) ColumnDefinitionBuffer(c IColumnSpec) bytes.Buffer {
	var buf bytes.Buffer
	buf.WriteByte('`')
	buf.WriteString(c.Name())
	buf.WriteByte('`')
	buf.WriteByte(' ')
	buf.WriteString(c.ColType())

	extra := c.ExtraDefs()
	if len(extra) > 0 {
		buf.WriteString(" ")
		buf.WriteString(extra)
	}

	if !c.IsNullable() {
		buf.WriteString(" NOT NULL")
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

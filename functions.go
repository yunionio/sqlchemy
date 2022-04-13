package sqlchemy

import (
	"fmt"
	"strings"
)

type SFunctionField struct {
	fields   []IQueryField
	function string
	alias    string
}

func (ff *SFunctionField) Expression() string {
	fieldRefs := make([]interface{}, 0)
	for _, f := range ff.fields {
		fieldRefs = append(fieldRefs, f.Reference())
	}
	return fmt.Sprintf("%s AS %s", fmt.Sprintf(ff.function, fieldRefs...), ff.Name())
}

func (ff *SFunctionField) Name() string {
	return ff.alias
}

func (ff *SFunctionField) Reference() string {
	return ff.alias
}

func (ff *SFunctionField) Label(label string) IQueryField {
	if len(label) > 0 && label != ff.alias {
		ff.alias = label
	}
	return ff
}

func NewFunctionField(name string, funcexp string, fields ...IQueryField) SFunctionField {
	ff := SFunctionField{function: funcexp, alias: name, fields: fields}
	return ff
}

func COUNT(name string) IQueryField {
	ff := NewFunctionField(name, "COUNT(*)")
	return &ff
}

func MAX(name string, field IQueryField) IQueryField {
	ff := NewFunctionField(name, "MAX(%s)", field)
	return &ff
}

func SUM(name string, field IQueryField) IQueryField {
	ff := NewFunctionField(name, "SUM(%s)", field)
	return &ff
}

func DISTINCT(name string, field IQueryField) IQueryField {
	ff := NewFunctionField(name, "DISTINCT(%s)", field)
	return &ff
}

func bc(name, op string, fields ...IQueryField) IQueryField {
	exps := []string{}
	for i := 0; i < len(fields); i++ {
		exps = append(exps, "%s")
	}
	ff := NewFunctionField(name, strings.Join(exps, fmt.Sprintf(" %s ", op)), fields...)
	return &ff
}

func ADD(name string, fields ...IQueryField) IQueryField {
	return bc(name, "+", fields...)
}

func SUB(name string, fields ...IQueryField) IQueryField {
	return bc(name, "-", fields...)
}

func MUL(name string, fields ...IQueryField) IQueryField {
	return bc(name, "*", fields...)
}

func DIV(name string, fields ...IQueryField) IQueryField {
	return bc(name, "/", fields...)
}

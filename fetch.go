package sqlchemy

import (
	"reflect"

	"yunion.io/x/pkg/gotypes"
	"yunion.io/x/pkg/util/reflectutils"
)

func (ts *STableSpec) Fetch(dt interface{}) error {
	q := ts.Query()
	dataValue := reflect.ValueOf(dt).Elem()
	fields := reflectutils.FetchStructFieldValueSet(dataValue)
	for _, c := range ts.columns {
		priVal, _ := fields.GetInterface(c.Name())
		if c.IsPrimary() && !gotypes.IsNil(priVal) { // skip update primary key
			q = q.Equals(c.Name(), priVal)
		}
	}
	return q.First(dt)
}

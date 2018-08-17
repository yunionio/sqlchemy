package sqlchemy

import (
	"fmt"
	"reflect"
	"testing"

	"yunion.io/x/pkg/util/reflectutils"
)

func insertSqlPrep(v interface{}) (string, []interface{}, error) {
	vvvalue := reflect.ValueOf(v).Elem()
	vv := vvvalue.Interface()
	vvFields := reflectutils.FetchStructFieldNameValueInterfaces(vvvalue)
	ts := NewTableSpecFromStruct(vv, "vv")
	fmt.Printf("%d", len(ts.columns))
	sql, vals, err := ts.insertSqlPrep(vvFields)
	return sql, vals, err
}

func TestInsertAutoIncrement(t *testing.T) {
	sql, vals, err := insertSqlPrep(&struct {
		RowId int `auto_increment:true`
	}{})
	if err != nil {
		t.Errorf("prepare sql failed: %s", err)
		return
	}
	wantSql := "INSERT INTO `vv` () VALUES()"
	if sql != wantSql {
		t.Errorf("sql want: %s\ngot: %s", wantSql, sql)
		return
	}
	if len(vals) != 0 {
		t.Errorf("vals want %d, got %d", 0, len(vals))
		return
	}
}

func TestInsertMultiAutoIncrement(t *testing.T) {
	defer func() {
		v := recover()
		if v == nil {
			t.Errorf("should panic with multiple auto_increment fields")
		}
	}()
	_, _, err := insertSqlPrep(&struct {
		RowId  int `auto_increment:true`
		RowId2 int `auto_increment:true`
	}{})
	t.Errorf("should panic but it continues: err: %s", err)
}

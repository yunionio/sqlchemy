package sqlchemy

import (
	"testing"
)

func TestBadColumns(t *testing.T) {
	wantPanic := func(t *testing.T, msgFmt string, msgVals ...interface{}) {
		if msg := recover(); msg == nil {
			t.Errorf(msgFmt, msgVals...)
		}
	}
	isPtr := false

	t.Run("bool default true", func(t *testing.T) {
		defer wantPanic(t, "non-pointer boolean must not have default value")
		NewBooleanColumn(
			"bad_column",
			map[string]string{
				"default": "1",
			},
			isPtr,
		)
	})
	t.Run("text with default", func(t *testing.T) {
		defer wantPanic(t, "ERROR 1101 (42000): BLOB/TEXT column 'xxx' can't have a default value")
		col := NewTextColumn(
			"bad",
			map[string]string{
				"default": "off",
			},
			isPtr,
		)
		def := col.DefinitionString()
		if def != "" {
			t.Fatal("should have paniced")
		}
	})
}

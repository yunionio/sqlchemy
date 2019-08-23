package sqlchemy

import (
	"testing"
)

func TestBoolDefaultTrue(t *testing.T) {
	defer func() {
		if msg := recover(); msg == nil {
			t.Errorf("non-pointer boolean must not have default value")
		}
	}()
	NewBooleanColumn(
		"bad_column",
		map[string]string{
			"default": "1",
		},
		false,
	)
}

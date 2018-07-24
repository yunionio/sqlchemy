package sqlchemy

import "testing"

func Test(t *testing.T) {
	t.Log(decodeSqlTypeString("VARCHAR(128)"))
	t.Log(decodeSqlTypeString("VARCHAR"))
	t.Log(decodeSqlTypeString("DECIMAL(10,2)"))
}

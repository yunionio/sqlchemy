package sqlchemy

import "testing"

func TestConditions(t *testing.T) {
	field := &SRawQueryField{"name"}
	cond1 := Equals(field, "zone1")
	t.Logf("%s %s", cond1.WhereClause(), cond1.Variables())
	cond2 := Equals(field, "zone2")
	t.Logf("%s %s", cond2.WhereClause(), cond2.Variables())
	cond3 := OR(cond1, cond2)
	t.Logf("%s %s", cond3.WhereClause(), cond3.Variables())
	cond4 := Equals(field, "zone3")
	cond5 := AND(cond4, cond3)
	t.Logf("%s %s", cond5.WhereClause(), cond5.Variables())
	cond6 := IsFalse(field)
	cond7 := AND(cond6, cond5)
	t.Logf("%s %s", cond7.WhereClause(), cond7.Variables())
	cond8 := AND(cond5, cond7)
	t.Logf("%s %s", cond8.WhereClause(), cond8.Variables())
}

// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlchemy

// Filter method filters a SQL query with given ICondition
// equivalent to add a clause in where conditions
func (tq *SQuery) Filter(cond ICondition) *SQuery {
	if tq.groupBy != nil && len(tq.groupBy) > 0 {
		if tq.having == nil {
			tq.having = cond
		} else {
			tq.having = AND(tq.having, cond)
		}
	} else {
		if tq.where == nil {
			tq.where = cond
		} else {
			tq.where = AND(tq.where, cond)
		}
	}
	return tq
}

// FilterByTrue filters query with a true condition
func (tq *SQuery) FilterByTrue() *SQuery {
	return tq.Filter(&STrueCondition{})
}

// FilterByFalse filters query with a false condition
func (tq *SQuery) FilterByFalse() *SQuery {
	return tq.Filter(&SFalseCondition{})
}

// Like filters query with a like condition
func (q *SQuery) Like(f string, v string) *SQuery {
	cond := Like(q.Field(f), v)
	return q.Filter(cond)
}

// Contains filters query with a contains condition
func (q *SQuery) Contains(f string, v string) *SQuery {
	cond := Contains(q.Field(f), v)
	return q.Filter(cond)
}

// Startswith filters query with a startswith condition
func (q *SQuery) Startswith(f string, v string) *SQuery {
	cond := Startswith(q.Field(f), v)
	return q.Filter(cond)
}

// Endswith filters query with a endswith condition
func (q *SQuery) Endswith(f string, v string) *SQuery {
	cond := Endswith(q.Field(f), v)
	return q.Filter(cond)
}

// NotLike filters query with a not like condition
func (q *SQuery) NotLike(f string, v string) *SQuery {
	cond := Like(q.Field(f), v)
	return q.Filter(NOT(cond))
}

// In filters query with a in condition
func (q *SQuery) In(f string, v interface{}) *SQuery {
	cond := In(q.Field(f), v)
	return q.Filter(cond)
}

// NotIn filters query with a not in condition
func (q *SQuery) NotIn(f string, v interface{}) *SQuery {
	cond := In(q.Field(f), v)
	return q.Filter(NOT(cond))
}

// Between filters query with a between condition
func (q *SQuery) Between(f string, v1, v2 interface{}) *SQuery {
	cond := Between(q.Field(f), v1, v2)
	return q.Filter(cond)
}

// NotBetween fitlers query with a not between condition
func (q *SQuery) NotBetween(f string, v1, v2 interface{}) *SQuery {
	cond := Between(q.Field(f), v1, v2)
	return q.Filter(NOT(cond))
}

// Equals filters query with a equals condition
func (q *SQuery) Equals(f string, v interface{}) *SQuery {
	cond := Equals(q.Field(f), v)
	return q.Filter(cond)
}

// NotEquals filters the query with a not equals condition
func (q *SQuery) NotEquals(f string, v interface{}) *SQuery {
	cond := NotEquals(q.Field(f), v)
	return q.Filter(cond)
}

// GE filters the query with a >= condition
func (q *SQuery) GE(f string, v interface{}) *SQuery {
	cond := GE(q.Field(f), v)
	return q.Filter(cond)
}

// LE filters the query with a <= condition
func (q *SQuery) LE(f string, v interface{}) *SQuery {
	cond := LE(q.Field(f), v)
	return q.Filter(cond)
}

// GT filters the query with a > condition
func (q *SQuery) GT(f string, v interface{}) *SQuery {
	cond := GT(q.Field(f), v)
	return q.Filter(cond)
}

// LT filters the query with a < condition
func (q *SQuery) LT(f string, v interface{}) *SQuery {
	cond := LT(q.Field(f), v)
	return q.Filter(cond)
}

// IsNull filters the query with a is null condition
func (q *SQuery) IsNull(f string) *SQuery {
	cond := IsNull(q.Field(f))
	return q.Filter(cond)
}

// IsNotNull filters the query with a is not null condition
func (q *SQuery) IsNotNull(f string) *SQuery {
	cond := IsNotNull(q.Field(f))
	return q.Filter(cond)
}

// IsEmpty filters the query with a is_empty condition
func (q *SQuery) IsEmpty(f string) *SQuery {
	cond := IsEmpty(q.Field(f))
	return q.Filter(cond)
}

// IsNullOrEmpty filters the query with a is null or empty condition
func (q *SQuery) IsNullOrEmpty(f string) *SQuery {
	cond := IsNullOrEmpty(q.Field(f))
	return q.Filter(cond)
}

// IsNotEmpty filters the query with a is not empty condition
func (q *SQuery) IsNotEmpty(f string) *SQuery {
	cond := IsNotEmpty(q.Field(f))
	return q.Filter(cond)
}

// IsTrue filters the query with a is true condition
func (q *SQuery) IsTrue(f string) *SQuery {
	cond := IsTrue(q.Field(f))
	return q.Filter(cond)
}

// IsFalse filters the query with a is false condition
func (q *SQuery) IsFalse(f string) *SQuery {
	cond := IsFalse(q.Field(f))
	return q.Filter(cond)
}

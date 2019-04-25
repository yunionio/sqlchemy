package sqlchemy

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

func (q *SQuery) Like(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Like(field, v)
	return q.Filter(cond)
}

func (q *SQuery) Contains(f string, v string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Contains(field, v)
	return q.Filter(cond)
}

func (q *SQuery) Startswith(f string, v string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Startswith(field, v)
	return q.Filter(cond)
}

func (q *SQuery) Endswith(f string, v string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Endswith(field, v)
	return q.Filter(cond)
}

func (q *SQuery) NotLike(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Like(field, v)
	return q.Filter(NOT(cond))
}

func (q *SQuery) In(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := In(field, v)
	return q.Filter(cond)
}

func (q *SQuery) NotIn(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := In(field, v)
	return q.Filter(NOT(cond))
}

func (q *SQuery) Between(f string, v1, v2 interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Between(field, v1, v2)
	return q.Filter(cond)
}

func (q *SQuery) NotBetween(f string, v1, v2 interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Between(field, v1, v2)
	return q.Filter(NOT(cond))
}

func (q *SQuery) Equals(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := Equals(field, v)
	return q.Filter(cond)
}

func (q *SQuery) NotEquals(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := NotEquals(field, v)
	return q.Filter(cond)
}

func (q *SQuery) GE(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := GE(field, v)
	return q.Filter(cond)
}

func (q *SQuery) LE(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := LE(field, v)
	return q.Filter(cond)
}

func (q *SQuery) GT(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := GT(field, v)
	return q.Filter(cond)
}

func (q *SQuery) LT(f string, v interface{}) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := LT(field, v)
	return q.Filter(cond)
}

func (q *SQuery) IsNull(f string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := IsNull(field)
	return q.Filter(cond)
}

func (q *SQuery) IsNotNull(f string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := IsNotNull(field)
	return q.Filter(cond)
}

func (q *SQuery) IsEmpty(f string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := IsEmpty(field)
	return q.Filter(cond)
}

func (q *SQuery) IsNullOrEmpty(f string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := IsNullOrEmpty(field)
	return q.Filter(cond)
}

func (q *SQuery) IsNotEmpty(f string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := IsNotEmpty(field)
	return q.Filter(cond)
}

func (q *SQuery) IsTrue(f string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := IsTrue(field)
	return q.Filter(cond)
}

func (q *SQuery) IsFalse(f string) *SQuery {
	field := q.Field(f)
	if field == nil {
		return q
	}
	cond := IsFalse(field)
	return q.Filter(cond)
}

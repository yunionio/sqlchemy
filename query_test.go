package sqlchemy

import (
	"reflect"
	"testing"

	"yunion.io/x/pkg/errors"
)

func TestQueryString(t *testing.T) {
	SetupMockDatabaseBackend()

	type TableStruct struct {
		Id     int    `json:"id" primary:"true"`
		Name   string `width:"16"`
		Age    int    `nullable:"true"`
		IsMale bool   `nullalbe:"true"`
	}
	table := NewTableSpecFromStruct(TableStruct{}, "testtable")
	cases := []struct {
		query *SQuery
		want  string
		vars  int
	}{
		{
			query: table.Instance().Query().Equals("id", 1),
			want:  "SELECT `t1`.`id`, `t1`.`name`, `t1`.`age`, `t1`.`is_male` FROM `testtable` AS `t1` WHERE `t1`.`id` = ( ? )",
			vars:  1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(t.Field("name", "newname"))
				q = q.Equals("id", 2)
				return q
			}(),
			want: "SELECT `t2`.`name` as `newname` FROM `testtable` AS `t2` WHERE `t2`.`id` = ( ? )",
			vars: 1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(t.Field("name")).Equals("id", 2).Asc(t.Field("name")).Limit(10).Offset(2)
				return q
			}(),
			want: "SELECT `t3`.`name` FROM `testtable` AS `t3` WHERE `t3`.`id` = ( ? ) ORDER BY `t3`.`name` ASC LIMIT 10 OFFSET 2",
			vars: 1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(t.Field("name"), COUNT("namecnt", t.Field("name")))
				q = q.GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT `t4`.`name`, COUNT(`t4`.`name`) AS `namecnt` FROM `testtable` AS `t4` GROUP BY `t4`.`name`",
			vars: 0,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q1 := t.Query(t.Field("id")).Equals("id", 2)
				q := t.Query(t.Field("name")).In("id", q1.SubQuery())
				return q
			}(),
			want: "SELECT `t5`.`name` FROM `testtable` AS `t5` WHERE `t5`.`id` IN (SELECT `t5`.`id` FROM `testtable` AS `t5` WHERE `t5`.`id` = ( ? ))",
			vars: 1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q1 := t.Query(t.Field("id")).Equals("id", 2)
				subq := q1.SubQuery()
				q2 := t.Query()
				q2 = q2.Join(subq, Equals(subq.Field("id"), q2.Field("id")))
				q2 = q2.NotEquals("name", "Hohn")
				return q2
			}(),
			want: "SELECT `t7`.`id`, `t7`.`name`, `t7`.`age`, `t7`.`is_male` FROM `testtable` AS `t7` JOIN (SELECT `t7`.`id` FROM `testtable` AS `t7` WHERE `t7`.`id` = ( ? )) AS `t8` ON `t8`.`id` = `t7`.`id` WHERE `t7`.`name` <> ( ? )",
			vars: 2,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q1 := t.Query(t.Field("name")).Equals("id", 1)
				q2 := t.Query(t.Field("name")).Equals("id", 2)
				uq, _ := UnionWithError(q1, q2)
				q3 := t.Query(t.Field("id")).In("name", uq.Query().SubQuery())
				return q3
			}(),
			want: "SELECT `t9`.`id` FROM `testtable` AS `t9` WHERE `t9`.`name` IN (SELECT `t10`.`name` FROM (SELECT `t55`.`name` FROM (SELECT `t9`.`name` FROM `testtable` AS `t9` WHERE `t9`.`id` = ( ? )) AS `t55` UNION SELECT `t56`.`name` FROM (SELECT `t9`.`name` FROM `testtable` AS `t9` WHERE `t9`.`id` = ( ? )) AS `t56`) AS `t10`)",
			vars: 2,
		},
		{
			query: table.Instance().Query().FilterByFalse(),
			want:  "SELECT `t12`.`id`, `t12`.`name`, `t12`.`age`, `t12`.`is_male` FROM `testtable` AS `t12` WHERE 0",
		},
		{
			query: table.Instance().Query().FilterByTrue(),
			want:  "SELECT `t13`.`id`, `t13`.`name`, `t13`.`age`, `t13`.`is_male` FROM `testtable` AS `t13` WHERE 1",
		},
		{
			query: table.Instance().Query().Like("name", "%abc%"),
			want:  "SELECT `t14`.`id`, `t14`.`name`, `t14`.`age`, `t14`.`is_male` FROM `testtable` AS `t14` WHERE `t14`.`name` LIKE ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().Contains("name", "abc"),
			want:  "SELECT `t15`.`id`, `t15`.`name`, `t15`.`age`, `t15`.`is_male` FROM `testtable` AS `t15` WHERE `t15`.`name` LIKE ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().Startswith("name", "abc"),
			want:  "SELECT `t16`.`id`, `t16`.`name`, `t16`.`age`, `t16`.`is_male` FROM `testtable` AS `t16` WHERE `t16`.`name` LIKE ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().Endswith("name", "abc"),
			want:  "SELECT `t17`.`id`, `t17`.`name`, `t17`.`age`, `t17`.`is_male` FROM `testtable` AS `t17` WHERE `t17`.`name` LIKE ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().NotLike("name", "abc"),
			want:  "SELECT `t18`.`id`, `t18`.`name`, `t18`.`age`, `t18`.`is_male` FROM `testtable` AS `t18` WHERE NOT (`t18`.`name` LIKE ( ? ))",
			vars:  1,
		},
		{
			query: table.Instance().Query().In("name", []string{"abc", "123"}),
			want:  "SELECT `t19`.`id`, `t19`.`name`, `t19`.`age`, `t19`.`is_male` FROM `testtable` AS `t19` WHERE `t19`.`name` IN ( ?, ? )",
			vars:  2,
		},
		{
			query: table.Instance().Query().NotIn("name", []string{"abc", "123"}),
			want:  "SELECT `t20`.`id`, `t20`.`name`, `t20`.`age`, `t20`.`is_male` FROM `testtable` AS `t20` WHERE NOT (`t20`.`name` IN ( ?, ? ))",
			vars:  2,
		},
		{
			query: table.Instance().Query().Between("name", "abc", "123"),
			want:  "SELECT `t21`.`id`, `t21`.`name`, `t21`.`age`, `t21`.`is_male` FROM `testtable` AS `t21` WHERE `t21`.`name` BETWEEN ( ? ) AND ( ? )",
			vars:  2,
		},
		{
			query: table.Instance().Query().NotBetween("name", "abc", "123"),
			want:  "SELECT `t22`.`id`, `t22`.`name`, `t22`.`age`, `t22`.`is_male` FROM `testtable` AS `t22` WHERE NOT (`t22`.`name` BETWEEN ( ? ) AND ( ? ))",
			vars:  2,
		},
		{
			query: table.Instance().Query().Equals("name", "abc"),
			want:  "SELECT `t23`.`id`, `t23`.`name`, `t23`.`age`, `t23`.`is_male` FROM `testtable` AS `t23` WHERE `t23`.`name` = ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().NotEquals("name", "abc"),
			want:  "SELECT `t24`.`id`, `t24`.`name`, `t24`.`age`, `t24`.`is_male` FROM `testtable` AS `t24` WHERE `t24`.`name` <> ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().GE("age", 20),
			want:  "SELECT `t25`.`id`, `t25`.`name`, `t25`.`age`, `t25`.`is_male` FROM `testtable` AS `t25` WHERE `t25`.`age` >= ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().LE("age", 20),
			want:  "SELECT `t26`.`id`, `t26`.`name`, `t26`.`age`, `t26`.`is_male` FROM `testtable` AS `t26` WHERE `t26`.`age` <= ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().GT("age", 20),
			want:  "SELECT `t27`.`id`, `t27`.`name`, `t27`.`age`, `t27`.`is_male` FROM `testtable` AS `t27` WHERE `t27`.`age` > ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().LT("age", 20),
			want:  "SELECT `t28`.`id`, `t28`.`name`, `t28`.`age`, `t28`.`is_male` FROM `testtable` AS `t28` WHERE `t28`.`age` < ( ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().IsNull("age"),
			want:  "SELECT `t29`.`id`, `t29`.`name`, `t29`.`age`, `t29`.`is_male` FROM `testtable` AS `t29` WHERE `t29`.`age` IS NULL",
		},
		{
			query: table.Instance().Query().IsNotNull("age"),
			want:  "SELECT `t30`.`id`, `t30`.`name`, `t30`.`age`, `t30`.`is_male` FROM `testtable` AS `t30` WHERE `t30`.`age` IS NOT NULL",
		},
		{
			query: table.Instance().Query().IsEmpty("name"),
			want:  "SELECT `t31`.`id`, `t31`.`name`, `t31`.`age`, `t31`.`is_male` FROM `testtable` AS `t31` WHERE LENGTH(`t31`.`name`) = 0",
		},
		{
			query: table.Instance().Query().IsNotEmpty("name"),
			want:  "SELECT `t32`.`id`, `t32`.`name`, `t32`.`age`, `t32`.`is_male` FROM `testtable` AS `t32` WHERE `t32`.`name` IS NOT NULL AND LENGTH(`t32`.`name`) > 0",
		},
		{
			query: table.Instance().Query().IsNullOrEmpty("name"),
			want:  "SELECT `t33`.`id`, `t33`.`name`, `t33`.`age`, `t33`.`is_male` FROM `testtable` AS `t33` WHERE `t33`.`name` IS NULL OR LENGTH(`t33`.`name`) = 0",
		},
		{
			query: table.Instance().Query().IsTrue("is_male"),
			want:  "SELECT `t34`.`id`, `t34`.`name`, `t34`.`age`, `t34`.`is_male` FROM `testtable` AS `t34` WHERE `t34`.`is_male` = 1",
		},
		{
			query: table.Instance().Query().IsFalse("is_male"),
			want:  "SELECT `t35`.`id`, `t35`.`name`, `t35`.`age`, `t35`.`is_male` FROM `testtable` AS `t35` WHERE `t35`.`is_male` = 0",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(MAX("max_id", t.Field("id"))).GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT MAX(`t36`.`id`) AS `max_id` FROM `testtable` AS `t36` GROUP BY `t36`.`name`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(MIN("min_id", t.Field("id"))).GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT MIN(`t37`.`id`) AS `min_id` FROM `testtable` AS `t37` GROUP BY `t37`.`name`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(SUM("male_cnt", t.Field("is_male"))).GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT SUM(`t38`.`is_male`) AS `male_cnt` FROM `testtable` AS `t38` GROUP BY `t38`.`name`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(DISTINCT("name", t.Field("name")))
				return q
			}(),
			want: "SELECT DISTINCT(`t39`.`name`) AS `name` FROM `testtable` AS `t39`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(GROUP_CONCAT("names", t.Field("name")))
				return q
			}(),
			want: "SELECT GROUP_CONCAT(`t40`.`name`) AS `names` FROM `testtable` AS `t40`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(REPLACE("new_name", t.Field("name"), "abc", "123")).GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT REPLACE(`t41`.`name`, \"abc\", \"123\") AS `new_name` FROM `testtable` AS `t41` GROUP BY `t41`.`name`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(NewConstField("MALE").Label("Gender")).IsTrue("is_male")
				return q
			}(),
			want: "SELECT \"MALE\" AS `Gender` FROM `testtable` AS `t42` WHERE `t42`.`is_male` = 1",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				subq := t.Query(NewStringField("FEMALE").Label("Gender")).IsFalse("is_male").SubQuery()
				q := subq.Query(COUNT("count", subq.Field("Gender")))
				return q
			}(),
			want: "SELECT COUNT(`t44`.`Gender`) AS `count` FROM (SELECT \"FEMALE\" AS `Gender` FROM `testtable` AS `t43` WHERE `t43`.`is_male` = 0) AS `t44`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(SUBSTR("name2", t.Field("name"), 0, 2)).GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT SUBSTR(`t45`.`name`, 0, 2) AS `name2` FROM `testtable` AS `t45` GROUP BY `t45`.`name`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(CONCAT("name_age", t.Field("name"), CAST(t.Field("age"), "VARCHAR", "")))
				return q
			}(),
			want: "SELECT CONCAT(`t46`.`name`,CAST(`t46`.`age` AS VARCHAR)) AS `name_age` FROM `testtable` AS `t46`",
		},
		{
			query: NewRawQuery("show create table `testtable`", "abc"),
			want:  "show create table `testtable`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(NewFunction(NewCase().When(IsTrue(t.Field("is_male")), NewStringField("MALE")).Else(NewStringField("FEMALE")), "Gender"))
				return q
			}(),
			want: "SELECT CASE WHEN `t47`.`is_male` = 1 THEN \"MALE\" ELSE \"FEMALE\" END AS `Gender` FROM `testtable` AS `t47`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q1 := t.Query(t.Field("name")).Equals("id", 1)
				q2 := t.Query(t.Field("name")).Equals("id", 2)
				uq, _ := UnionAllWithError(q1, q2)
				q3 := t.Query(t.Field("id")).In("name", uq.Query().SubQuery())
				return q3
			}(),
			want: "SELECT `t48`.`id` FROM `testtable` AS `t48` WHERE `t48`.`name` IN (SELECT `t49`.`name` FROM (SELECT `t59`.`name` FROM (SELECT `t48`.`name` FROM `testtable` AS `t48` WHERE `t48`.`id` = ( ? )) AS `t59` UNION ALL SELECT `t60`.`name` FROM (SELECT `t48`.`name` FROM `testtable` AS `t48` WHERE `t48`.`id` = ( ? )) AS `t60`) AS `t49`)",
			vars: 2,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q1 := t.Query(t.Field("name")).Equals("id", 1)
				q2 := t.Query()
				subq1 := q1.SubQuery()
				q2 = q2.Join(subq1, In(q2.Field("name"), subq1.Field("name")))
				q2 = q2.Equals("name", "John")
				return q2
			}(),
			want: "SELECT `t51`.`id`, `t51`.`name`, `t51`.`age`, `t51`.`is_male` FROM `testtable` AS `t51` JOIN (SELECT `t51`.`name` FROM `testtable` AS `t51` WHERE `t51`.`id` = ( ? )) AS `t52` ON `t51`.`name` IN `t52`.`name` WHERE `t51`.`name` = ( ? )",
			vars: 2,
		},
	}
	for _, c := range cases {
		got := c.query.String()
		if got != c.want {
			t.Errorf("want: %s got: %s", c.want, got)
		}
		vars := c.query.Variables()
		if len(vars) != c.vars {
			t.Errorf("want vars: %d got %d", c.vars, len(vars))
		}
	}
}

type arrayScanner []interface{}

func (as arrayScanner) Scan(target ...interface{}) error {
	for i := range target {
		if i >= len(as) {
			return errors.Error("out of range")
		}
		targetValue := reflect.ValueOf(target[i]).Elem()
		targetValue.Set(reflect.ValueOf(as[i]))
	}
	return nil
}

func TestRowScan2StringMap(t *testing.T) {
	cases := []struct {
		fields  []string
		row     arrayScanner
		wantMap map[string]string
	}{
		{
			fields: []string{"name", "age"},
			row: arrayScanner{
				"John",
				20,
			},
			wantMap: map[string]string{
				"name": "John",
				"age":  "20",
			},
		},
		{
			fields: []string{"name", "age", "is_male"},
			row: arrayScanner{
				"John",
				20,
				true,
			},
			wantMap: map[string]string{
				"name":    "John",
				"age":     "20",
				"is_male": "true",
			},
		},
	}
	for _, c := range cases {
		strmap, err := rowScan2StringMap(c.fields, c.row)
		if err != nil {
			t.Errorf("rowScan2StringMap fail %s", err)
		} else {
			if !reflect.DeepEqual(strmap, c.wantMap) {
				t.Errorf("want: %s got: %s", c.wantMap, strmap)
			}
		}
	}
}

func TestMapString2Struct(t *testing.T) {
	type testStruct struct {
		Name   string `json:"name"`
		Age    int    `json:"age"`
		IsMale bool   `json:"is_male"`
	}
	cases := []struct {
		mapStr map[string]string
		want   interface{}
		dest   interface{}
	}{
		{
			mapStr: map[string]string{
				"name":    "John",
				"age":     "20",
				"is_male": "true",
			},
			dest: &testStruct{},
			want: &testStruct{
				Name:   "John",
				Age:    20,
				IsMale: true,
			},
		},
	}
	for _, c := range cases {
		err := mapString2Struct(c.mapStr, reflect.ValueOf(c.dest).Elem())
		if err != nil {
			t.Errorf("mapString2Struct fail %s", err)
		} else {
			if !reflect.DeepEqual(c.want, c.dest) {
				t.Errorf("want: %#v got: %#v", c.want, c.dest)
			}
		}
	}
}

func TestQueryString2(t *testing.T) {
	SetupMockDatabaseBackend()
	ResetTableID()

	type TableStruct struct {
		Id      int    `json:"id" primary:"true"`
		Name    string `width:"16"`
		IpStart string `width:"64"`
		IpEnd   string `width:"64"`
	}
	table := NewTableSpecFromStruct(TableStruct{}, "testtable")
	cases := []struct {
		query *SQuery
		want  string
		vars  int
	}{
		{
			query: table.Instance().Query().Equals("id", 1),
			want:  "SELECT `t1`.`id`, `t1`.`name`, `t1`.`ip_start`, `t1`.`ip_end` FROM `testtable` AS `t1` WHERE `t1`.`id` = ( ? )",
			vars:  1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query()
				q = q.Filter(Between(NewStringField("192.168.0.1"), q.Field("ip_start"), q.Field("ip_end")))
				return q
			}(),
			want: "SELECT `t2`.`id`, `t2`.`name`, `t2`.`ip_start`, `t2`.`ip_end` FROM `testtable` AS `t2` WHERE \"192.168.0.1\" BETWEEN `t2`.`ip_start` AND `t2`.`ip_end`",
			vars: 0,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query()
				q = q.Filter(Between(NewConstField("192.168.0.1"), q.Field("ip_start"), q.Field("ip_end")))
				return q
			}(),
			want: "SELECT `t3`.`id`, `t3`.`name`, `t3`.`ip_start`, `t3`.`ip_end` FROM `testtable` AS `t3` WHERE \"192.168.0.1\" BETWEEN `t3`.`ip_start` AND `t3`.`ip_end`",
			vars: 0,
		},
	}
	for _, c := range cases {
		got := c.query.String()
		if got != c.want {
			t.Errorf("want: %s got: %s", c.want, got)
		}
		vars := c.query.Variables()
		if len(vars) != c.vars {
			t.Errorf("want vars: %d got %d", c.vars, len(vars))
		}
	}
}

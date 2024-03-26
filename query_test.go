package sqlchemy

import (
	"reflect"
	"testing"
	"time"

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
			want:  "SELECT `t1`.`id` AS `id`, `t1`.`name` AS `name`, `t1`.`age` AS `age`, `t1`.`is_male` AS `is_male` FROM `testtable` AS `t1` WHERE `t1`.`id` =  ? ",
			vars:  1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(t.Field("name", "newname"))
				q = q.Equals("id", 2)
				return q
			}(),
			want: "SELECT `t2`.`name` AS `newname` FROM `testtable` AS `t2` WHERE `t2`.`id` =  ? ",
			vars: 1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(t.Field("name")).Equals("id", 2).Asc(t.Field("name")).Limit(10).Offset(2)
				return q
			}(),
			want: "SELECT `t3`.`name` AS `name` FROM `testtable` AS `t3` WHERE `t3`.`id` =  ?  ORDER BY `t3`.`name` ASC LIMIT 10 OFFSET 2",
			vars: 1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(t.Field("name"), COUNT("namecnt", t.Field("name")))
				q = q.GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT `t4`.`name` AS `name`, COUNT(`t4`.`name`) AS `namecnt` FROM `testtable` AS `t4` GROUP BY `t4`.`name`",
			vars: 0,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q1 := t.Query(t.Field("id")).Equals("id", 2)
				q := t.Query(t.Field("name")).In("id", q1.SubQuery())
				return q
			}(),
			want: "SELECT `t5`.`name` AS `name` FROM `testtable` AS `t5` WHERE `t5`.`id` IN (SELECT `t5`.`id` AS `id` FROM `testtable` AS `t5` WHERE `t5`.`id` =  ? )",
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
			want: "SELECT `t7`.`id` AS `id`, `t7`.`name` AS `name`, `t7`.`age` AS `age`, `t7`.`is_male` AS `is_male` FROM `testtable` AS `t7` JOIN (SELECT `t7`.`id` AS `id` FROM `testtable` AS `t7` WHERE `t7`.`id` =  ? ) AS `t8` ON `t8`.`id` = `t7`.`id` WHERE `t7`.`name` <>  ? ",
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
			want: "SELECT `t9`.`id` AS `id` FROM `testtable` AS `t9` WHERE `t9`.`name` IN (SELECT `t10`.`name` AS `name` FROM (SELECT `t63`.`name` AS `name` FROM (SELECT `t9`.`name` AS `name` FROM `testtable` AS `t9` WHERE `t9`.`id` =  ? ) AS `t63` UNION SELECT `t64`.`name` AS `name` FROM (SELECT `t9`.`name` AS `name` FROM `testtable` AS `t9` WHERE `t9`.`id` =  ? ) AS `t64`) AS `t10`)",
			vars: 2,
		},
		{
			query: table.Instance().Query().FilterByFalse(),
			want:  "SELECT `t12`.`id` AS `id`, `t12`.`name` AS `name`, `t12`.`age` AS `age`, `t12`.`is_male` AS `is_male` FROM `testtable` AS `t12` WHERE 0",
		},
		{
			query: table.Instance().Query().FilterByTrue(),
			want:  "SELECT `t13`.`id` AS `id`, `t13`.`name` AS `name`, `t13`.`age` AS `age`, `t13`.`is_male` AS `is_male` FROM `testtable` AS `t13` WHERE 1",
		},
		{
			query: table.Instance().Query().Like("name", "%abc%"),
			want:  "SELECT `t14`.`id` AS `id`, `t14`.`name` AS `name`, `t14`.`age` AS `age`, `t14`.`is_male` AS `is_male` FROM `testtable` AS `t14` WHERE `t14`.`name` LIKE  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().Contains("name", "abc"),
			want:  "SELECT `t15`.`id` AS `id`, `t15`.`name` AS `name`, `t15`.`age` AS `age`, `t15`.`is_male` AS `is_male` FROM `testtable` AS `t15` WHERE `t15`.`name` LIKE  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().Startswith("name", "abc"),
			want:  "SELECT `t16`.`id` AS `id`, `t16`.`name` AS `name`, `t16`.`age` AS `age`, `t16`.`is_male` AS `is_male` FROM `testtable` AS `t16` WHERE `t16`.`name` LIKE  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().Endswith("name", "abc"),
			want:  "SELECT `t17`.`id` AS `id`, `t17`.`name` AS `name`, `t17`.`age` AS `age`, `t17`.`is_male` AS `is_male` FROM `testtable` AS `t17` WHERE `t17`.`name` LIKE  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().NotLike("name", "abc"),
			want:  "SELECT `t18`.`id` AS `id`, `t18`.`name` AS `name`, `t18`.`age` AS `age`, `t18`.`is_male` AS `is_male` FROM `testtable` AS `t18` WHERE NOT (`t18`.`name` LIKE  ? )",
			vars:  1,
		},
		{
			query: table.Instance().Query().In("name", []string{"abc", "123"}),
			want:  "SELECT `t19`.`id` AS `id`, `t19`.`name` AS `name`, `t19`.`age` AS `age`, `t19`.`is_male` AS `is_male` FROM `testtable` AS `t19` WHERE `t19`.`name` IN ( ?, ? )",
			vars:  2,
		},
		{
			query: table.Instance().Query().NotIn("name", []string{"abc", "123"}),
			want:  "SELECT `t20`.`id` AS `id`, `t20`.`name` AS `name`, `t20`.`age` AS `age`, `t20`.`is_male` AS `is_male` FROM `testtable` AS `t20` WHERE `t20`.`name` NOT IN ( ?, ? )",
			vars:  2,
		},
		{
			query: table.Instance().Query().Between("name", "abc", "123"),
			want:  "SELECT `t21`.`id` AS `id`, `t21`.`name` AS `name`, `t21`.`age` AS `age`, `t21`.`is_male` AS `is_male` FROM `testtable` AS `t21` WHERE `t21`.`name` BETWEEN  ?  AND  ? ",
			vars:  2,
		},
		{
			query: table.Instance().Query().NotBetween("name", "abc", "123"),
			want:  "SELECT `t22`.`id` AS `id`, `t22`.`name` AS `name`, `t22`.`age` AS `age`, `t22`.`is_male` AS `is_male` FROM `testtable` AS `t22` WHERE NOT (`t22`.`name` BETWEEN  ?  AND  ? )",
			vars:  2,
		},
		{
			query: table.Instance().Query().Equals("name", "abc"),
			want:  "SELECT `t23`.`id` AS `id`, `t23`.`name` AS `name`, `t23`.`age` AS `age`, `t23`.`is_male` AS `is_male` FROM `testtable` AS `t23` WHERE `t23`.`name` =  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().NotEquals("name", "abc"),
			want:  "SELECT `t24`.`id` AS `id`, `t24`.`name` AS `name`, `t24`.`age` AS `age`, `t24`.`is_male` AS `is_male` FROM `testtable` AS `t24` WHERE `t24`.`name` <>  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().GE("age", 20),
			want:  "SELECT `t25`.`id` AS `id`, `t25`.`name` AS `name`, `t25`.`age` AS `age`, `t25`.`is_male` AS `is_male` FROM `testtable` AS `t25` WHERE `t25`.`age` >=  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().LE("age", 20),
			want:  "SELECT `t26`.`id` AS `id`, `t26`.`name` AS `name`, `t26`.`age` AS `age`, `t26`.`is_male` AS `is_male` FROM `testtable` AS `t26` WHERE `t26`.`age` <=  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().GT("age", 20),
			want:  "SELECT `t27`.`id` AS `id`, `t27`.`name` AS `name`, `t27`.`age` AS `age`, `t27`.`is_male` AS `is_male` FROM `testtable` AS `t27` WHERE `t27`.`age` >  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().LT("age", 20),
			want:  "SELECT `t28`.`id` AS `id`, `t28`.`name` AS `name`, `t28`.`age` AS `age`, `t28`.`is_male` AS `is_male` FROM `testtable` AS `t28` WHERE `t28`.`age` <  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().IsNull("age"),
			want:  "SELECT `t29`.`id` AS `id`, `t29`.`name` AS `name`, `t29`.`age` AS `age`, `t29`.`is_male` AS `is_male` FROM `testtable` AS `t29` WHERE `t29`.`age` IS NULL",
		},
		{
			query: table.Instance().Query().IsNotNull("age"),
			want:  "SELECT `t30`.`id` AS `id`, `t30`.`name` AS `name`, `t30`.`age` AS `age`, `t30`.`is_male` AS `is_male` FROM `testtable` AS `t30` WHERE `t30`.`age` IS NOT NULL",
		},
		{
			query: table.Instance().Query().IsEmpty("name"),
			want:  "SELECT `t31`.`id` AS `id`, `t31`.`name` AS `name`, `t31`.`age` AS `age`, `t31`.`is_male` AS `is_male` FROM `testtable` AS `t31` WHERE LENGTH(`t31`.`name`) = 0 OR LENGTH(`t31`.`name`) IS NULL",
		},
		{
			query: table.Instance().Query().IsNotEmpty("name"),
			want:  "SELECT `t32`.`id` AS `id`, `t32`.`name` AS `name`, `t32`.`age` AS `age`, `t32`.`is_male` AS `is_male` FROM `testtable` AS `t32` WHERE `t32`.`name` IS NOT NULL AND LENGTH(`t32`.`name`) > 0",
		},
		{
			query: table.Instance().Query().IsNullOrEmpty("name"),
			want:  "SELECT `t33`.`id` AS `id`, `t33`.`name` AS `name`, `t33`.`age` AS `age`, `t33`.`is_male` AS `is_male` FROM `testtable` AS `t33` WHERE `t33`.`name` IS NULL OR LENGTH(`t33`.`name`) = 0 OR LENGTH(`t33`.`name`) IS NULL",
		},
		{
			query: table.Instance().Query().IsTrue("is_male"),
			want:  "SELECT `t34`.`id` AS `id`, `t34`.`name` AS `name`, `t34`.`age` AS `age`, `t34`.`is_male` AS `is_male` FROM `testtable` AS `t34` WHERE `t34`.`is_male` = 1",
		},
		{
			query: table.Instance().Query().IsFalse("is_male"),
			want:  "SELECT `t35`.`id` AS `id`, `t35`.`name` AS `name`, `t35`.`age` AS `age`, `t35`.`is_male` AS `is_male` FROM `testtable` AS `t35` WHERE `t35`.`is_male` = 0",
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
			want: "SELECT GROUP_CONCAT(`t40`.`name` SEPARATOR ',') AS `names` FROM `testtable` AS `t40`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(REPLACE("new_name", t.Field("name"), "abc", "123")).GroupBy(t.Field("name"))
				q = q.AppendField(t.Field("name"))
				return q
			}(),
			want: "SELECT MAX(REPLACE(`t41`.`name`, \"abc\", \"123\")) AS `new_name`, `t41`.`name` AS `name` FROM `testtable` AS `t41` GROUP BY `t41`.`name`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(NewConstField("MALE").Label("Gender")).IsTrue("is_male")
				return q
			}(),
			want: "SELECT 'MALE' AS `Gender` FROM `testtable` AS `t42` WHERE `t42`.`is_male` = 1",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				subq := t.Query(NewStringField("FEMALE").Label("Gender")).IsFalse("is_male").SubQuery()
				q := subq.Query(COUNT("count", subq.Field("Gender")))
				return q
			}(),
			want: "SELECT COUNT(`t44`.`Gender`) AS `count` FROM (SELECT 'FEMALE' AS `Gender` FROM `testtable` AS `t43` WHERE `t43`.`is_male` = 0) AS `t44`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(SUBSTR("name2", t.Field("name"), 0, 2)).GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT MAX(SUBSTR(`t45`.`name`, 0, 2)) AS `name2` FROM `testtable` AS `t45` GROUP BY `t45`.`name`",
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
				q := t.Query(NewFunction(NewCase().When(IsTrue(t.Field("is_male")), NewStringField("MALE")).Else(NewStringField("FEMALE")), "Gender", false))
				return q
			}(),
			want: "SELECT CASE WHEN `t47`.`is_male` = 1 THEN 'MALE' ELSE 'FEMALE' END AS `Gender` FROM `testtable` AS `t47`",
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
			want: "SELECT `t48`.`id` AS `id` FROM `testtable` AS `t48` WHERE `t48`.`name` IN (SELECT `t49`.`name` AS `name` FROM (SELECT `t67`.`name` AS `name` FROM (SELECT `t48`.`name` AS `name` FROM `testtable` AS `t48` WHERE `t48`.`id` =  ? ) AS `t67` UNION ALL SELECT `t68`.`name` AS `name` FROM (SELECT `t48`.`name` AS `name` FROM `testtable` AS `t48` WHERE `t48`.`id` =  ? ) AS `t68`) AS `t49`)",
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
			want: "SELECT `t51`.`id` AS `id`, `t51`.`name` AS `name`, `t51`.`age` AS `age`, `t51`.`is_male` AS `is_male` FROM `testtable` AS `t51` JOIN (SELECT `t51`.`name` AS `name` FROM `testtable` AS `t51` WHERE `t51`.`id` =  ? ) AS `t52` ON `t51`.`name` IN `t52`.`name` WHERE `t51`.`name` =  ? ",
			vars: 2,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(LOWER("lower_name", t.Field("name")))
				return q
			}(),
			want: "SELECT LOWER(`t53`.`name`) AS `lower_name` FROM `testtable` AS `t53`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(UPPER("upper_name", t.Field("name")))
				return q
			}(),
			want: "SELECT UPPER(`t54`.`name`) AS `upper_name` FROM `testtable` AS `t54`",
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(GROUP_CONCAT2("names", ":", t.Field("name")))
				return q
			}(),
			want: "SELECT GROUP_CONCAT(`t55`.`name` SEPARATOR ':') AS `names` FROM `testtable` AS `t55`",
		},
		{
			query: table.Instance().Query().In("name", []string{"abc"}),
			want:  "SELECT `t56`.`id` AS `id`, `t56`.`name` AS `name`, `t56`.`age` AS `age`, `t56`.`is_male` AS `is_male` FROM `testtable` AS `t56` WHERE `t56`.`name` =  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().NotIn("name", []string{"abc"}),
			want:  "SELECT `t57`.`id` AS `id`, `t57`.`name` AS `name`, `t57`.`age` AS `age`, `t57`.`is_male` AS `is_male` FROM `testtable` AS `t57` WHERE `t57`.`name` <>  ? ",
			vars:  1,
		},
		{
			query: table.Instance().Query().In("name", []string{}),
			want:  "SELECT `t58`.`id` AS `id`, `t58`.`name` AS `name`, `t58`.`age` AS `age`, `t58`.`is_male` AS `is_male` FROM `testtable` AS `t58` WHERE 0",
			vars:  0,
		},
		{
			query: table.Instance().Query().NotIn("name", []string{}),
			want:  "SELECT `t59`.`id` AS `id`, `t59`.`name` AS `name`, `t59`.`age` AS `age`, `t59`.`is_male` AS `is_male` FROM `testtable` AS `t59` WHERE 1",
			vars:  0,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query(SUM("total_age", t.Field("age")), t.Field("name"), t.Field("id")).GroupBy(t.Field("name"))
				return q
			}(),
			want: "SELECT SUM(`t60`.`age`) AS `total_age`, `t60`.`name` AS `name`, MAX(`t60`.`id`) AS `id` FROM `testtable` AS `t60` GROUP BY `t60`.`name`",
			vars: 0,
		},
	}
	for _, c := range cases {
		got := c.query.String()
		if got != c.want {
			t.Errorf("want: %s", c.want)
			t.Errorf(" got: %s", got)
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
			want:  "SELECT `t1`.`id` AS `id`, `t1`.`name` AS `name`, `t1`.`ip_start` AS `ip_start`, `t1`.`ip_end` AS `ip_end` FROM `testtable` AS `t1` WHERE `t1`.`id` =  ? ",
			vars:  1,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query()
				q = q.Filter(Between(NewStringField("192.168.0.1"), q.Field("ip_start"), q.Field("ip_end")))
				return q
			}(),
			want: "SELECT `t2`.`id` AS `id`, `t2`.`name` AS `name`, `t2`.`ip_start` AS `ip_start`, `t2`.`ip_end` AS `ip_end` FROM `testtable` AS `t2` WHERE '192.168.0.1' BETWEEN `t2`.`ip_start` AND `t2`.`ip_end`",
			vars: 0,
		},
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query()
				q = q.Filter(Between(NewConstField("192.168.0.1"), q.Field("ip_start"), q.Field("ip_end")))
				return q
			}(),
			want: "SELECT `t3`.`id` AS `id`, `t3`.`name` AS `name`, `t3`.`ip_start` AS `ip_start`, `t3`.`ip_end` AS `ip_end` FROM `testtable` AS `t3` WHERE '192.168.0.1' BETWEEN `t3`.`ip_start` AND `t3`.`ip_end`",
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

func TestQueryString3(t *testing.T) {
	SetupMockDatabaseBackend()
	ResetTableID()

	type TableStruct struct {
		Id        int       `json:"id" primary:"true"`
		StartTime time.Time `json:"start_time"`
		EndTime   time.Time `json:"end_time"`
	}
	table := NewTableSpecFromStruct(TableStruct{}, "testtable")
	cases := []struct {
		query *SQuery
		want  string
		vars  int
	}{
		{
			query: func() *SQuery {
				t := table.Instance()
				q := t.Query()
				q = q.Filter(GE(DATEDIFF("year", q.Field("start_time"), q.Field("end_time")), 1))
				return q
			}(),
			want: "SELECT `t1`.`id` AS `id`, `t1`.`start_time` AS `start_time`, `t1`.`end_time` AS `end_time` FROM `testtable` AS `t1` WHERE DATEDIFF('year',`t1`.`start_time`,`t1`.`end_time`) >=  ? ",
			vars: 1,
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

func getInstance(tableName string, reset bool) *SQuery {
	if reset {
		ResetTableID()
	}
	type TableStruct struct {
		Id     int     `json:"id" primary:"true"`
		Name   string  `width:"16"`
		Age    int     `nullable:"true"`
		IsMale bool    `nullalbe:"true"`
		Brand  string  `width:"32"`
		Amount float64 `default:"0"`
	}
	table := NewTableSpecFromStruct(TableStruct{}, tableName)
	q := table.Query()
	cols := table.Columns()
	for i := range cols {
		if cols[i].Name() == "amount" {
			amountField := NewFunction(NewCase().When(Equals(q.Field("brand"), "AWS"), MUL("", q.Field("amount"), NewConstField(6.0))).Else(q.Field("amount")), "amount", false)
			q = q.AppendField(amountField)
		}
	}
	return q.SubQuery().Query()
}

func TestQueryString4(t *testing.T) {
	SetupMockDatabaseBackend()
	cases := []struct {
		query *SQuery
		want  string
		vars  int
	}{
		{
			query: func() *SQuery {
				q := getInstance("testtable", true)
				q = q.AppendField(q.Field("id"))
				q = q.AppendField(q.Field("name"))
				q = q.AppendField(q.Field("brand"))
				q = q.AppendField(q.Field("amount"))
				return q
			}(),
			want: "SELECT `t2`.`id` AS `id`, `t2`.`name` AS `name`, `t2`.`brand` AS `brand`, `t2`.`amount` AS `amount` FROM (SELECT CASE WHEN `t1`.`brand` =  ?  THEN `t1`.`amount` * 6.000000 ELSE `t1`.`amount` END AS `amount`, `t1`.`brand` AS `brand`, `t1`.`id` AS `id`, `t1`.`name` AS `name` FROM `testtable` AS `t1`) AS `t2`",
			vars: 1,
		},
		{
			query: func() *SQuery {
				q := getInstance("testtable", true)
				q = q.SubQuery().Query()
				q = q.GroupBy(q.Field("brand"))
				q = q.AppendField(q.Field("brand"))
				q = q.AppendField(SUM("total_amount", q.Field("amount")))
				return q
			}(),
			want: "SELECT `t3`.`brand` AS `brand`, SUM(`t3`.`amount`) AS `total_amount` FROM (SELECT `t2`.`amount` AS `amount`, `t2`.`brand` AS `brand` FROM (SELECT CASE WHEN `t1`.`brand` =  ?  THEN `t1`.`amount` * 6.000000 ELSE `t1`.`amount` END AS `amount`, `t1`.`brand` AS `brand` FROM `testtable` AS `t1`) AS `t2`) AS `t3` GROUP BY `t3`.`brand`",
			vars: 1,
		},
		{
			query: func() *SQuery {
				q := getInstance("payment_bills_tbl", true)
				q = q.Equals("brand", "AWS")
				q2 := getInstance("recal_bills_tbl", false).SubQuery()
				q = q.Join(q2, Equals(q.Field("id"), q2.Field("id")))
				q = q.GroupBy(q.Field("brand"))
				q = q.AppendField(q.Field("brand"))
				q = q.AppendField(SUM("total_amount", q.Field("amount")))
				q = q.AppendField(SUM("total_amount_sec", q2.Field("amount")))
				return q
			}(),
			want: "SELECT `t2`.`brand` AS `brand`, SUM(`t2`.`amount`) AS `total_amount`, SUM(`t5`.`amount`) AS `total_amount_sec` FROM (SELECT CASE WHEN `t1`.`brand` =  ?  THEN `t1`.`amount` * 6.000000 ELSE `t1`.`amount` END AS `amount`, `t1`.`brand` AS `brand`, `t1`.`id` AS `id` FROM `payment_bills_tbl` AS `t1`) AS `t2` JOIN (SELECT `t4`.`amount` AS `amount`, `t4`.`id` AS `id` FROM (SELECT CASE WHEN `t3`.`brand` =  ?  THEN `t3`.`amount` * 6.000000 ELSE `t3`.`amount` END AS `amount`, `t3`.`id` AS `id` FROM `recal_bills_tbl` AS `t3`) AS `t4`) AS `t5` ON `t2`.`id` = `t5`.`id` WHERE `t2`.`brand` =  ?  GROUP BY `t2`.`brand`",
			vars: 3,
		},
		{
			query: func() *SQuery {
				q := getInstance("payment_bills_tbl", true)
				q = q.Equals("brand", "AWS")
				q2 := getInstance("recal_bills_tbl", false).SubQuery()
				q = q.Join(q2, Equals(q.Field("id"), q2.Field("id")))
				q = q.AppendField(q.Field("amount"))
				q = q.AppendField(q2.Field("amount").Label("amount_sec"))
				q = q.SubQuery().Query()
				q = q.GroupBy(q.Field("brand"))
				q = q.AppendField(q.Field("brand"))
				q = q.AppendField(SUM("total_amount", q.Field("amount")))
				q = q.AppendField(SUM("total_amount_sec", q.Field("amount_sec")))
				return q
			}(),
			want: "SELECT `t6`.`brand` AS `brand`, SUM(`t6`.`amount`) AS `total_amount`, SUM(`t6`.`amount_sec`) AS `total_amount_sec` FROM (SELECT `t2`.`amount` AS `amount`, `t5`.`amount` AS `amount_sec`, `t2`.`brand` AS `brand` FROM (SELECT CASE WHEN `t1`.`brand` =  ?  THEN `t1`.`amount` * 6.000000 ELSE `t1`.`amount` END AS `amount`, `t1`.`brand` AS `brand`, `t1`.`id` AS `id` FROM `payment_bills_tbl` AS `t1`) AS `t2` JOIN (SELECT `t4`.`amount` AS `amount`, `t4`.`id` AS `id` FROM (SELECT CASE WHEN `t3`.`brand` =  ?  THEN `t3`.`amount` * 6.000000 ELSE `t3`.`amount` END AS `amount`, `t3`.`id` AS `id` FROM `recal_bills_tbl` AS `t3`) AS `t4`) AS `t5` ON `t2`.`id` = `t5`.`id` WHERE `t2`.`brand` =  ? ) AS `t6` GROUP BY `t6`.`brand`",
			vars: 3,
		},
	}
	for _, c := range cases {
		got := c.query.String()
		if got != c.want {
			t.Errorf("want:\n%s\ngot:\n%s\n", c.want, got)
		}
		vars := c.query.Variables()
		if len(vars) != c.vars {
			t.Errorf("want vars: %d got %d", c.vars, len(vars))
		}
	}
}

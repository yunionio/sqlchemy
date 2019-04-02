package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang-plus/uuid"
	"reflect"
	"time"

	"yunion.io/x/jsonutils"
	"yunion.io/x/log"
	"yunion.io/x/pkg/gotypes"
	"yunion.io/x/pkg/util/timeutils"
	"yunion.io/x/sqlchemy"
)

func uuid4() string {
	uid, _ := uuid.NewV4()
	return uid.String()
}

func now() string {
	return timeutils.MysqlTime(time.Time{})
}

type SCompondStruct struct {
	Id  string
	Age int
}

func (self *SCompondStruct) IsZero() bool {
	return len(self.Id) == 0 && self.Age == 0
}

func (self *SCompondStruct) Equals(obj gotypes.ISerializable) bool {
	comp, ok := obj.(*SCompondStruct)
	if !ok {
		return false
	}
	return self.Age == comp.Age && self.Id == comp.Id
}

func (self *SCompondStruct) String() string {
	return jsonutils.Marshal(self).String()
}

func init() {
	comp := SCompondStruct{}
	compType := reflect.TypeOf(&comp)
	gotypes.RegisterSerializable(compType, func() gotypes.ISerializable {
		return &SCompondStruct{}
	})
}

type TestTable struct {
	Id        string               `primary:"true" width:"128" charset:"ascii" nullable:"false"`
	Name      string               `width:"64" charset:"utf8" index:"true"`
	Gender    string               `width:"10" charset:"ascii"`
	Age       uint8                `default:"18"`
	Info      jsonutils.JSONObject `nullable:"false"`
	Compond   *SCompondStruct      `width:1024`
	CreatedAt time.Time            `nullable:"false" created_at:"true"`
	UpdatedAt time.Time            `nullable:"false" updated_at:"true"`
	Version   int                  `default:"0" nullable:"false" auto_version:"true"`
	DeletedAt time.Time            ``
	Deleted   bool                 `nullable:"false" default:"false"`
	Notes     string               `default:"default notes"`
}

type Ticket struct {
	Id     int       `auto_increment:"true"`
	UserId string    `width:"128" charset:"ascii" nullable:"false"`
	Date   time.Time `nullable:"false"`
}

func (t *TestTable) BeforeInsert() {
	t.Id = uuid4()
	dict := jsonutils.NewDict()
	dict.Add(jsonutils.NewString("Test"), "name")
	t.Info = dict
	t.Compond = &SCompondStruct{Id: "123456", Age: 24}
}

func (t *Ticket) BeforeInsert() {
	t.Date = time.Now().UTC()
}

type AgentTable struct {
	UserId string `primary:"true" width:"128" charset:"ascii"`
	Age    int
}

func main() {
	db, err := sql.Open("mysql", "testgo:openstack@tcp(127.0.0.1:3306)/testgo?charset=utf8&parseTime")
	if err != nil {
		panic(fmt.Sprintf("Open DB failed: %s", err))
	}
	sqlchemy.SetDB(db)
	defer sqlchemy.CloseDB()

	tablespec := sqlchemy.NewTableSpecFromStruct(TestTable{}, "testtable")
	tablespec.CheckSync()

	agespec := sqlchemy.NewTableSpecFromStruct(AgentTable{}, "age_tbl")
	agespec.CheckSync()

	ticketSpec := sqlchemy.NewTableSpecFromStruct(Ticket{}, "ticket_tbl")
	ticketSpec.CheckSync()

	t1 := tablespec.Instance()
	t2 := agespec.Instance()
	// t3 := ticketSpec.Instance()
	q := t1.Query(t1.Field("id"), t2.Field("age")).
		Join(t2, sqlchemy.AND(sqlchemy.Equals(t1.Field("id"), t2.Field("user_id")), sqlchemy.GE(t2.Field("age"), 20))).
		Filter(sqlchemy.Like(t1.Field("Id"), "123%")).
		Limit(10).
		Asc(t2.Field("age"))
	fmt.Println(q.String())
	fmt.Println(q.Variables())

	subq := q.SubQuery()

	q2 := subq.Query().Desc("id")
	fmt.Println(q2.String())
	fmt.Println(q2.Variables())

	dt1 := TestTable{}
	dt1.Name = "Test"
	// dt1.Notes = "not null notes"
	err = tablespec.Insert(&dt1)
	if err != nil {
		log.Errorf("Insert error: %s", err)
	}

	fmt.Println("dt1 after insert: ", dt1)

	dt2 := Ticket{}
	dt2.UserId = dt1.Id
	err = ticketSpec.Insert(&dt2)
	if err != nil {
		log.Errorf("Insert ticket fail %s", err)
	}
	fmt.Println(dt2)

	count := q.Count()
	fmt.Println("Count: ", count)
	count = q2.Count()
	fmt.Println("Count: ", count)

	q = t1.Query().Desc(t1.Field("created_at")).IsNotEmpty("info").IsNotNull("compond").Limit(10)
	mapData, err := q.AllStringMap()
	if err != nil {
		log.Errorf("query first %s", err)
	} else {
		for _, v := range mapData {
			fmt.Println(v)
		}
	}

	dt3 := TestTable{}
	err = q.First(&dt3)
	if err != nil {
		log.Errorf("First error %s", err)
	} else {
		fmt.Println("Before update", dt3)
	}

	/*session, err := tablespec.PrepareUpdate(dt3)
	if err != nil {
		log.Errorf("Fail to prepare update %s", err)
	}else {
		dt3.Name = "New name 4"
		dt3.Compond = &SCompondStruct{Id:"998822333", Age: 80}
		// dt3.Compond.Age = 80
		// dt3.Compond.Id = "998822333"
		diff, err := session.SaveUpdate(dt3)
		if err != nil {
			log.Errorf("SaveUpdate fail %s", err)
		}else {
			log.Infof("Update difference: %s", sqlchemy.UpdateDiffString(diff))
		}
	}*/

	_, err = tablespec.Update(&dt3, func() error {
		dt3.Name = "New name 4"
		dt3.Compond = &SCompondStruct{Id: "998822333", Age: 80}
		return nil
	})
	if err != nil {
		log.Errorf("update fail %s", err)
	}

	q = t1.Query().Equals("id", dt3.Id)
	err = q.First(&dt3)
	if err != nil {
		log.Errorf("First error %s", err)
	} else {
		fmt.Println("After update: ", jsonutils.Marshal(dt3))
	}

	log.Infof("Start SQuery ALL")

	dt4 := make([]TestTable, 0)
	err = q.GT("version", 0).All(&dt4)
	if err != nil {
		log.Errorf("query all fail %s", err)
	} else {
		log.Infof("SQuery all no error %d", len(dt4))
		for _, v := range dt4 {
			fmt.Println("dt4 ", jsonutils.Marshal(v))
		}
	}

	qId1 := t1.Query(t1.Field("id"))
	t3 := ticketSpec.Instance()
	qId2 := t3.Query(t3.Field("id"))

	union, err := sqlchemy.Union(qId1, qId2)
	if err != nil {
		log.Errorf("fail to union %s", err)
	} else {
		q := union.Limit(20).Offset(10).Desc("id").SubQuery().Query()
		fmt.Println(q.String())

		type sID struct {
			Id string
		}
		idList := make([]sID, 0)
		err := q.All(&idList)
		if err != nil {
			log.Errorf("fail to query idList %s", err)
		} else {
			log.Infof("Test: %s", jsonutils.Marshal(idList))
		}
	}

}

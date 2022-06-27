module yunion.io/x/sqlchemy

go 1.16

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.1.0-1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang-plus/uuid v1.0.0
	github.com/mattn/go-sqlite3 v1.14.12
	golang.org/x/text v0.3.7
	yunion.io/x/jsonutils v0.0.0-20220106020632-953b71a4c3a8
	yunion.io/x/log v1.0.0
	yunion.io/x/pkg v1.0.0
)

replace (
	github.com/ClickHouse/clickhouse-go/v2 => github.com/yunionio/clickhouse-go/v2 v2.1.0-1
	github.com/go-logr/logr => github.com/go-logr/logr v0.4.0
)

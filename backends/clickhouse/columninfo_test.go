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

package clickhouse

import (
	"reflect"
	"testing"

	"yunion.io/x/onecloud/pkg/util/stringutils2"
)

func TestParseCreateTable(t *testing.T) {
	cases := []struct {
		in        string
		orderbys  []string
		primaries []string
		partition string
		ttl       sColumnTTL
	}{
		{
			in:        "CREATE TABLE test.testtable (`id` String) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) PRIMARY KEY (id, name) ORDER BY (id, name) SETTINGS index_granularity = 8192",
			orderbys:  []string{"id", "name"},
			primaries: []string{"id", "name"},
			partition: "toYYYYMM(created_at)",
			ttl:       sColumnTTL{},
		},
		{
			in:        "CREATE TABLE test.testtable (`id` String) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) PRIMARY KEY id ORDER BY id SETTINGS index_granularity = 8192",
			orderbys:  []string{"id"},
			primaries: []string{"id"},
			partition: "toYYYYMM(created_at)",
			ttl:       sColumnTTL{},
		},
		{
			in: `CREATE TABLE yunionmeter.payment_bills_tbl
			(created_at DateTime,
		)
			ENGINE = MergeTree
			PARTITION BY toInt32(day / 100)
			PRIMARY KEY day
			ORDER BY day
			TTL created_at + INTERVAL 3 MONTH
			SETTINGS index_granularity = 8192`,
			orderbys:  []string{"day"},
			primaries: []string{"day"},
			partition: "toInt32(day / 100)",
			ttl: sColumnTTL{ColName: "created_at",
				sTTL: sTTL{
					Count: 3,
					Unit:  "MONTH",
				}},
		},
		{
			in:        "CREATE TABLE yunionlogger.action_tbl (`id` Int64, `obj_type` String, `obj_id` String, `obj_name` String, `action` String, `notes` Nullable(String), `tenant_id` Nullable(String), `tenant` Nullable(String), `project_domain_id` Nullable(String) DEFAULT CAST('default', 'Nullable(String)'), `project_domain` Nullable(String) DEFAULT CAST('Default', 'Nullable(String)'), `user_id` Nullable(String), `user` Nullable(String), `domain_id` Nullable(String), `domain` Nullable(String), `roles` Nullable(String), `ops_time` DateTime, `owner_domain_id` Nullable(String) DEFAULT CAST('default', 'Nullable(String)'), `owner_tenant_id` Nullable(String), `start_time` Nullable(DateTime), `success` Nullable(UInt8), `service` Nullable(String)) ENGINE = MergeTree PARTITION BY toInt64(id / 100000000000) PRIMARY KEY id ORDER BY id TTL ops_time + toIntervalMonth(6) SETTINGS index_granularity = 8192",
			orderbys:  []string{"id"},
			primaries: []string{"id"},
			partition: "toInt64(id / 100000000000)",
			ttl: sColumnTTL{
				ColName: "ops_time",
				sTTL: sTTL{
					Count: 6,
					Unit:  "MONTH",
				},
			},
		},
	}
	for _, c := range cases {
		primaries, orderbys, partition, ttlStr := parseCreateTable(c.in)
		sortedPrimaries := stringutils2.NewSortedStrings(primaries)
		sortedOrderBys := stringutils2.NewSortedStrings(orderbys)
		sortedPrimaries2 := stringutils2.NewSortedStrings(c.primaries)
		sortedOrderBys2 := stringutils2.NewSortedStrings(c.orderbys)
		if !stringutils2.Equals(sortedPrimaries, sortedPrimaries2) {
			t.Errorf("primaries mismatch: want: %s got: %s", sortedPrimaries2, sortedPrimaries)
		}
		if !stringutils2.Equals(sortedOrderBys, sortedOrderBys2) {
			t.Errorf("orderby mismatch: want: %s got: %s", sortedOrderBys2, sortedOrderBys)
		}
		if partition != c.partition {
			t.Errorf("partition mismatch: want %s got %s", c.partition, partition)
		}
		if len(ttlStr) > 0 {
			ttlVal, err := parseTTLExpression(ttlStr)
			if err != nil {
				t.Errorf("parseTTLExpression %s fail %s", ttlStr, err)
			} else if !reflect.DeepEqual(ttlVal, c.ttl) {
				t.Errorf("parseTTLExpression want %v got %v", c.ttl, ttlVal)
			}
		}
	}
}

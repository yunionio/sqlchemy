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
	"testing"

	"yunion.io/x/onecloud/pkg/util/stringutils2"
)

func TestParseCreateTable(t *testing.T) {
	cases := []struct {
		in        string
		orderbys  []string
		primaries []string
		partition string
	}{
		{
			in:        "CREATE TABLE test.testtable (`id` String) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) PRIMARY KEY (id, name) ORDER BY (id, name) SETTINGS index_granularity = 8192",
			orderbys:  []string{"id", "name"},
			primaries: []string{"id", "name"},
			partition: "toYYYYMM(created_at)",
		},
		{
			in:        "CREATE TABLE test.testtable (`id` String) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) PRIMARY KEY id ORDER BY id SETTINGS index_granularity = 8192",
			orderbys:  []string{"id"},
			primaries: []string{"id"},
			partition: "toYYYYMM(created_at)",
		},
		{
			in: `CREATE TABLE yunionmeter.payment_bills_tbl
			(created_at DateTime,
		)
			ENGINE = MergeTree
			PARTITION BY toInt32(day / 100)
			PRIMARY KEY day
			ORDER BY day
			SETTINGS index_granularity = 8192`,
			orderbys:  []string{"day"},
			primaries: []string{"day"},
			partition: "toInt32(day / 100)",
		},
	}
	for _, c := range cases {
		primaries, orderbys, partition := parseCreateTable(c.in)
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
	}
}

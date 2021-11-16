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

import (
	"testing"

	"yunion.io/x/pkg/util/timeutils"
)

func TestSqlDebug(t *testing.T) {
	tm, _ := timeutils.ParseIsoTime("2021-11-01T12:00:00Z")
	cases := []struct {
		sql  string
		vars []interface{}
		want string
	}{
		{
			sql: `SET a = ?, b = ?, c = ?`,
			vars: []interface{}{
				"name",
				123,
				tm,
			},
			want: `SET a = 'name', b = 123, c = '2021-11-01 12:00:00 +0000 UTC'`,
		},
	}
	for _, c := range cases {
		got := _sqlDebug(c.sql, c.vars)
		if got != c.want {
			t.Errorf("want: %s got: %s", c.want, got)
		}
	}
}

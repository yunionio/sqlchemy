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

import "testing"

func TestParseTTL(t *testing.T) {
	cases := []struct {
		in   string
		want sTTL
	}{
		{
			in: "10m",
			want: sTTL{
				Count: 10,
				Unit:  "MONTH",
			},
		},
		{
			in: "1d",
			want: sTTL{
				Count: 1,
				Unit:  "DAY",
			},
		},
		{
			in: "24h",
			want: sTTL{
				Count: 24,
				Unit:  "HOUR",
			},
		},
	}
	for i, c := range cases {
		got, err := parseTTL(c.in)
		if err != nil {
			t.Errorf("[%d] parseTTL %s fail %s", i, c.in, err)
		} else {
			if got != c.want {
				t.Errorf("parseTTL %s want %v got %v", c.in, c.want, got)
			}
		}
	}
}

func TestParseTTLExpression(t *testing.T) {
	cases := []struct {
		in   string
		want sColumnTTL
	}{
		{
			in: "created_at + INTERVAL 3 MONTH",
			want: sColumnTTL{
				ColName: "created_at",
				sTTL: sTTL{
					Count: 3,
					Unit:  "MONTH",
				},
			},
		},
		{
			in: "`created_at` + INTERVAL 3 MONTH",
			want: sColumnTTL{
				ColName: "created_at",
				sTTL: sTTL{
					Count: 3,
					Unit:  "MONTH",
				},
			},
		},
		{
			in: "'created_at' + INTERVAL 100 DAY",
			want: sColumnTTL{
				ColName: "created_at",
				sTTL: sTTL{
					Count: 3,
					Unit:  "DAY",
				},
			},
		},
		{
			in: "ops_time + toIntervalMonth(6)",
			want: sColumnTTL{
				ColName: "ops_time",
				sTTL: sTTL{
					Count: 6,
					Unit:  "MONTH",
				},
			},
		},
		{
			in: "ops_time + toIntervalYear(1)",
			want: sColumnTTL{
				ColName: "ops_time",
				sTTL: sTTL{
					Count: 12,
					Unit:  "MONTH",
				},
			},
		},
	}
	for _, c := range cases {
		got, err := parseTTLExpression(c.in)
		if err != nil {
			t.Errorf("parseTTLExpression %s got %v want %v", c.in, got, c.want)
		}
	}
}

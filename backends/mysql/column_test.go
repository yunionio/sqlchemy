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

package mysql

import (
	"testing"
)

func TestBadColumns(t *testing.T) {
	wantPanic := func(t *testing.T, msgFmt string, msgVals ...interface{}) {
		if msg := recover(); msg == nil {
			t.Errorf(msgFmt, msgVals...)
		}
	}
	isPtr := false

	t.Run("bool default true", func(t *testing.T) {
		defer wantPanic(t, "non-pointer boolean must not have default value")
		NewBooleanColumn(
			"bad_column",
			map[string]string{
				"default": "1",
			},
			isPtr,
		)
	})
	t.Run("text with default", func(t *testing.T) {
		defer wantPanic(t, "ERROR 1101 (42000): BLOB/TEXT column 'xxx' can't have a default value")
		col := NewTextColumn(
			"bad",
			"TEXT",
			map[string]string{
				"default": "off",
			},
			isPtr,
		)
		def := col.DefinitionString()
		if def != "" {
			t.Fatal("should have paniced")
		}
	})
}

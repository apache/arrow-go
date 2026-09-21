// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitutil_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

func TestBitmapAllSetEmptyRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		buf  []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"unset", []byte{0}},
		{"set", []byte{0xff}},
	} {
		for _, offset := range []int{0, 1, 7, 8, 63, 64, 65, 1024} {
			t.Run(fmt.Sprintf("%s/offset=%d", tc.name, offset), func(t *testing.T) {
				if !bitutil.BitmapAllSet(tc.buf, offset, 0) {
					t.Fatal("an empty bitmap range must be all-set")
				}
			})
		}
	}
}

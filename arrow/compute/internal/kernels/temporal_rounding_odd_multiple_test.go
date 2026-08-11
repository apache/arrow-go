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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package kernels

import "testing"

func TestRoundToMultipleInt64OddMultipleAcrossModes(t *testing.T) {
	for _, mode := range []RoundMode{HalfDown, HalfUp, HalfToEven} {
		t.Run(mode.String(), func(t *testing.T) {
			for _, tc := range []struct {
				value int64
				want  int64
			}{
				{value: 1, want: 0},
				{value: 2, want: 3},
				{value: -1, want: 0},
				{value: -2, want: -3},
			} {
				got := roundToMultipleInt64(tc.value, 3, mode, false)
				if got != tc.want {
					t.Errorf("roundToMultipleInt64(%d, 3, %s) = %d, want %d", tc.value, mode, got, tc.want)
				}
			}
		})
	}
}

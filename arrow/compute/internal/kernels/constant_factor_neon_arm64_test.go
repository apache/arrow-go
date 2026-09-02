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

//go:build go1.18 && arm64 && !noasm && !appengine

package kernels

import (
	"math"
	"testing"
)

func TestNeonLengthFitsAssembly(t *testing.T) {
	tests := []struct {
		name   string
		length int
		want   bool
	}{
		{name: "maximum assembly length", length: math.MaxInt32, want: true},
		{name: "above maximum assembly length", length: math.MaxInt32 + 1, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := neonLengthFitsAssembly(tt.length); got != tt.want {
				t.Fatalf("neonLengthFitsAssembly(%d) = %t, want %t", tt.length, got, tt.want)
			}
		})
	}
}

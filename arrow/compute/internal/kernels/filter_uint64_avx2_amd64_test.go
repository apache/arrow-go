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

//go:build go1.18 && amd64 && !noasm && !appengine

package kernels

import (
	"testing"

	"golang.org/x/sys/cpu"
)

func TestFilterUint64Avx2AllMasks(t *testing.T) {
	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 is not available")
	}
	testFilterUint64AllMasks(t, filterUint64Avx2)
}

func TestFilterUint64Avx2AlignedOffset(t *testing.T) {
	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 is not available")
	}
	testFilterUint64AlignedOffset(t, filterUint64Avx2)
}

func TestFilterUint64Avx2Guards(t *testing.T) {
	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 is not available")
	}
	testFilterUint64Guards(t, filterUint64Avx2)
}

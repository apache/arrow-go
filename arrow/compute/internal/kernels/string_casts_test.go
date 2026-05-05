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

//go:build go1.18

package kernels

import (
	"errors"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestReserveFormattedDataOverflowGuard verifies that reserveFormattedData
// returns arrow.ErrInvalid (rather than panicking inside the builder's
// ReserveData) when (Len - Nulls) * perValueBytes would overflow int32.
// Tested at the kernel level so we don't need to drive >2 GB of input
// through the full executor to trigger the guard. See GH-184.
func TestReserveFormattedDataOverflowGuard(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewStringViewBuilder(mem)
	defer bldr.Release()

	span := &exec.ArraySpan{
		Len:   int64(math.MaxInt32/20) + 1,
		Nulls: 0,
	}

	err := reserveFormattedData(bldr, span, 20)
	if err == nil {
		t.Fatal("expected overflow guard to return an error")
	}
	if !errors.Is(err, arrow.ErrInvalid) {
		t.Fatalf("expected arrow.ErrInvalid, got %v", err)
	}
}

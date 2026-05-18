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

package compute

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/types"
)

// TestCoalesceArrayDataExtensionOverDictOfView verifies that when an
// extension's storage is dictionary<...view...>, coalesceArrayData
// preserves the Dictionary() member when it unwraps and rewraps the
// extension so the recursive DICTIONARY branch does not see a nil
// dictionary. Driven directly at the coalesce helper because the full
// cast pipeline for extension<dict<...>> hits a pre-existing SetMembers
// gap unrelated to view coalescing (exec.ArraySpan.SetMembers does not
// recognize a dictionary stored under an extension type).
func TestCoalesceArrayDataExtensionOverDictOfView(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	extType := types.NewDictStringViewExtType()

	const valBytes = 20000
	a := strings.Repeat("a", valBytes)
	b := strings.Repeat("b", valBytes)
	vbldr := array.NewStringViewBuilder(mem)
	defer vbldr.Release()
	vbldr.Append(a)
	vbldr.Append(b)
	vals := vbldr.NewArray()
	defer vals.Release()
	if n := len(vals.Data().Buffers()); n <= 3 {
		t.Fatalf("test precondition: dict values must be multi-buffer; got %d", n)
	}

	ibldr := array.NewInt32Builder(mem)
	defer ibldr.Release()
	ibldr.AppendValues([]int32{0, 1, 0, 1, 0}, nil)
	idx := ibldr.NewArray()
	defer idx.Release()

	valsData, ok := vals.Data().(*array.Data)
	if !ok {
		t.Fatalf("values data not *array.Data, got %T", vals.Data())
	}
	extData := array.NewDataWithDictionary(extType, idx.Len(),
		idx.Data().Buffers(), 0, 0, valsData)
	defer extData.Release()

	if !needsViewCoalesce(extData) {
		t.Fatal("needsViewCoalesce should return true for extension<dict<multi-buffer view>>")
	}

	coalesced, err := coalesceArrayData(mem, extData)
	if err != nil {
		t.Fatalf("coalesceArrayData returned an error: %v", err)
	}
	defer coalesced.Release()

	if !arrow.TypeEqual(coalesced.DataType(), extType) {
		t.Errorf("coalesced datatype mismatch: want %s, got %s", extType, coalesced.DataType())
	}
	dict := coalesced.Dictionary()
	if dict == nil {
		t.Fatal("coalesced extension<dict<view>> must preserve Dictionary()")
	}
	if got := len(dict.Buffers()); got > 3 {
		t.Errorf("coalesced dictionary values must be single-buffer; got %d buffers", got)
	}
}

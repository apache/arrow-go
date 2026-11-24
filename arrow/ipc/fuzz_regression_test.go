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

package ipc_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestStreamFuzzRegression(t *testing.T) {
	dataDir := os.Getenv("ARROW_TEST_DATA")
	if dataDir == "" {
		t.Skip("ARROW_TEST_DATA is not set")
	}
	dir := filepath.Join(dataDir, "arrow-ipc-stream")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Go reads these to the end without error or batches; Arrow C++ rejects both
	// (invalid flatbuffer, decimal precision 0), so tightening validation may flip them.
	clean := map[string]bool{
		"clusterfuzz-testcase-minimized-arrow-ipc-stream-fuzz-4757582821064704": true,
		"crash-5e88bae6ac5250714e8c8bc73b9d67b949fadbb4":                        true,
	}
	count := 0
	for _, entry := range entries {
		if !entry.Type().IsRegular() || entry.Name() == "README.md" {
			continue
		}
		count++
		t.Run(entry.Name(), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("%s: unexpected panic: %v", entry.Name(), p)
				}
			}()
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			r, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(mem))
			if err == nil {
				defer r.Release()
				for r.Next() {
				}
				err = r.Err()
			}
			if !clean[entry.Name()] && err == nil {
				t.Error("expected an error, got success")
			}
		})
	}
	if count == 0 {
		t.Fatal("no stream regression inputs found")
	}
}

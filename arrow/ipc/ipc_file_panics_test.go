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

// Fuzz inputs that used to panic in NewFileReader (apache/arrow-go#1568).
func TestFileReaderFuzzPanics(t *testing.T) {
	dataDir := os.Getenv("ARROW_TEST_DATA")
	if dataDir == "" {
		t.Skip("ARROW_TEST_DATA is not set")
	}
	for _, name := range []string{
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5577412021190656",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5693640900083712",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5864855240835072.fuzz",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5749190446153728",
		"clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5768644156194816",
		"clusterfuzz-testcase-arrow-ipc-file-fuzz-5768644156194816",
	} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dataDir, "arrow-ipc-file", name))
			if err != nil {
				t.Fatal(err)
			}
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			r, err := ipc.NewFileReader(bytes.NewReader(data), ipc.WithAllocator(mem))
			if err == nil {
				r.Close()
				t.Fatal("expected an error, got success")
			}
		})
	}
}

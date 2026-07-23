// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build arm64 && !noasm

package memory_test

import (
	"os"
	"os/exec"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNEONAssemblyTracebacks(t *testing.T) {
	// This is a regression test for GH-983. _memset_neon used to modify the
	// stack pointer in a way that the assembler didn't see, leading to
	// broken tracebacks in CPU profiler samples when the function is
	// running.

	buf := make([]byte, 1<<20)
	var traces []byte
	duration := time.Second
	d := t.TempDir()
	for range 5 {
		f, err := os.CreateTemp(d, "cpu.prof")
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			t.Skipf("CPU profiling is already enabled: %v", err)
		}

		deadline := time.Now().Add(duration)
		for time.Now().Before(deadline) {
			memory.Set(buf, 0x1f)
		}
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		traces, err = exec.Command("go", "tool", "pprof", "-traces", f.Name()).CombinedOutput()
		if err != nil {
			t.Fatalf("go tool pprof -traces: %v\n%s", err, traces)
		}
		if strings.Contains(string(traces), "memory._memset_neon") {
			break
		}
		// Mitigate potential flakiness on CI runners
		duration *= 2
	}
	if !strings.Contains(string(traces), "memory._memset_neon") {
		t.Fatal("CPU profile contains no _memset_neon samples after 5 attempts")
	}

	want := strings.Join([]string{
		"github.com/apache/arrow-go/v18/arrow/memory._memset_neon",
		"github.com/apache/arrow-go/v18/arrow/memory.memory_memset_neon",
		"github.com/apache/arrow-go/v18/arrow/memory.Set",
		"github.com/apache/arrow-go/v18/arrow/memory_test.TestNEONAssemblyTracebacks",
	}, "")

	for trace := range strings.SplitSeq(string(traces), "-----------") {
		if !strings.Contains(trace, "memory._memset_neon") {
			continue
		}

		// pprof adds whitespace and marks inlined functions, neither of which is
		// part of the call stack. Remove both before requiring adjacent frames.
		trace = strings.ReplaceAll(trace, "(inline)", "")
		trace = strings.Join(strings.Fields(trace), "")
		if !strings.Contains(trace, want) {
			t.Errorf("invalid _memset_neon traceback:\n%s", trace)
		}
	}
}

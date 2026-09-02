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

package arrgen_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/arrgen"
)

// TestCheckedInFilesAreUpToDate regenerates every committed arrgen output in
// this module and fails if the result differs from what is on disk.
//
// Generated code that is checked in drifts from its source the first time
// somebody edits a struct and forgets to rerun go generate, and the failure
// then shows up as a mysteriously wrong column rather than as a broken build.
// This turns that into a test failure at the moment of the edit.
func TestCheckedInFilesAreUpToDate(t *testing.T) {
	header, err := os.ReadFile("license_header.txt")
	if err != nil {
		t.Fatalf("reading license header: %v", err)
	}

	// These mirror the go:generate directives next to each type. Add a line
	// here whenever a new generated file is committed to this module.
	targets := []struct {
		dir   string
		types []string
		file  string
	}{
		{"example", []string{"Metric"}, "metric_arrow.go"},
		{"internal/gentypes", []string{"Row", "Fixed"}, "row_arrow.go"},
	}

	for _, target := range targets {
		t.Run(target.file, func(t *testing.T) {
			got, err := arrgen.Generate(arrgen.Config{
				Dir:    target.dir,
				Types:  target.types,
				Header: string(header),
			})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			path := filepath.Join(target.dir, target.file)
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s is out of date; run go generate ./...", path)
			}
		})
	}
}

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

// Command arrgen generates zero-reflection Arrow appenders for Go struct types.
//
// It reads the same arrow:"..." struct tags that
// github.com/apache/arrow-go/v18/arrow/array/arreflect interprets at runtime,
// but reads them once, when you run it, and writes typed Go source instead.
//
// Usage:
//
//	arrgen -type Metric [-type Other] [-output metric_arrow.go] [-header LICENSE.txt]
//
// Typically it is invoked through go:generate, next to the type:
//
//	//go:generate go run github.com/apache/arrow-go/arrgen/cmd/arrgen -type Metric
//
// Check the generated file in, as you would stringer or easyjson output.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/arrgen"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "arrgen: %v\n", err)
		os.Exit(1)
	}
}

// typeList collects -type values: both "-type A,B" and "-type A -type B" work.
type typeList []string

func (t *typeList) String() string { return strings.Join(*t, ",") }

func (t *typeList) Set(v string) error {
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			*t = append(*t, p)
		}
	}
	return nil
}

func run(argv []string) error {
	fs := flag.NewFlagSet("arrgen", flag.ContinueOnError)
	var types typeList
	fs.Var(&types, "type", "struct type name to generate for; repeatable, or comma-separated (required)")
	dir := fs.String("dir", ".", "directory of the package holding the types")
	output := fs.String("output", "", "output file, relative to -dir (default <first type>_arrow.go)")
	header := fs.String("header", "", "file whose contents are copied above the generated-code marker, e.g. a license header")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if len(types) == 0 {
		return fmt.Errorf("-type is required")
	}

	cfg := arrgen.Config{Dir: *dir, Types: types}
	if *header != "" {
		b, err := os.ReadFile(*header)
		if err != nil {
			return fmt.Errorf("reading -header: %w", err)
		}
		cfg.Header = string(b)
	}

	src, err := arrgen.Generate(cfg)
	if err != nil {
		return err
	}

	out := *output
	if out == "" {
		out = strings.ToLower(types[0]) + "_arrow.go"
	}
	path := filepath.Join(*dir, out)
	if err := os.WriteFile(path, src, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

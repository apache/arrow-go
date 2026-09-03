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

package arrgen

import (
	"bytes"
	"fmt"
	"go/format"
	"go/token"
	"go/types"
	"reflect"
	"strings"
	"unicode"

	"golang.org/x/tools/go/packages"
)

// Config describes one generator run.
type Config struct {
	// Dir is the directory of the package holding the struct types. go:generate
	// runs in the package directory, so the command leaves this as ".".
	Dir string
	// Types are the struct type names to generate for, in output order.
	Types []string
	// Header, when non-empty, is emitted verbatim above the generated-code
	// marker. Use it for a license header; it is expected to be Go comments.
	Header string
}

// Generate resolves cfg.Types in the package at cfg.Dir and returns the
// formatted source of their Arrow appenders. It never writes to disk, which
// keeps it usable from tests that only want to compare against a golden file.
//
// Output depends on nothing but the package's source, so regenerating an
// unchanged package reproduces the file byte for byte - the property the drift
// test in this module relies on.
func Generate(cfg Config) ([]byte, error) {
	if len(cfg.Types) == 0 {
		return nil, fmt.Errorf("arrgen: no types requested")
	}
	dir := cfg.Dir
	if dir == "" {
		dir = "."
	}
	pkg, loadErrs, err := loadPackage(dir)
	if err != nil {
		return nil, err
	}

	file := genFile{Header: strings.TrimRight(cfg.Header, "\n"), Package: pkg.Name}
	declared := make(map[string]string) // generated identifier -> type that claimed it
	for _, name := range cfg.Types {
		gt, err := genForType(pkg, name)
		if err != nil {
			return nil, withLoadContext(fmt.Errorf("arrgen: %s: %w", name, err), loadErrs)
		}
		if err := claimIdents(declared, name, gt); err != nil {
			return nil, fmt.Errorf("arrgen: %s: %w", name, err)
		}
		file.NeedTime = file.NeedTime || gt.needTime
		file.Types = append(file.Types, gt)
	}

	var buf bytes.Buffer
	if err := fileTemplate.Execute(&buf, file); err != nil {
		return nil, fmt.Errorf("arrgen: rendering template: %w", err)
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("arrgen: formatting generated source: %w\n--- raw ---\n%s", err, buf.String())
	}
	return src, nil
}

// loadPackage type-checks the package in dir and returns it along with any
// errors the type checker reported.
//
// Those errors are returned rather than raised, because the package a generator
// runs in usually does not compile yet: the code calling MetricRecordBatch is
// written before the file defining it exists. Refusing to run then would make
// the generator unusable exactly when it is needed. go/packages still resolves
// everything it can, so the struct being generated for is available; a load
// error only becomes the answer when resolving that struct fails, and
// withLoadContext then reports it as the likely cause.
func loadPackage(dir string) (*packages.Package, []string, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax,
		Dir:  dir,
	}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("arrgen: loading package in %s: %w", dir, err)
	}
	if len(pkgs) != 1 {
		return nil, nil, fmt.Errorf("arrgen: expected exactly one package in %s, found %d", dir, len(pkgs))
	}
	var errs []string
	for _, e := range pkgs[0].Errors {
		errs = append(errs, e.Error())
	}
	if pkgs[0].Types == nil {
		return nil, nil, fmt.Errorf("arrgen: package in %s could not be type-checked: %s", dir, strings.Join(errs, "; "))
	}
	return pkgs[0], errs, nil
}

// withLoadContext appends the package's type errors to err, since a failure to
// resolve a type in a package that does not compile is usually a symptom of
// that rather than of the struct itself.
func withLoadContext(err error, loadErrs []string) error {
	if len(loadErrs) == 0 {
		return err
	}
	const maxShown = 5
	shown := loadErrs
	suffix := ""
	if len(shown) > maxShown {
		shown, suffix = shown[:maxShown], fmt.Sprintf("\n  ... and %d more", len(loadErrs)-maxShown)
	}
	return fmt.Errorf("%w\nthe package does not type-check, which may be the cause:\n  %s%s",
		err, strings.Join(shown, "\n  "), suffix)
}

func genForType(pkg *packages.Package, name string) (genType, error) {
	st, err := findStruct(pkg, name)
	if err != nil {
		return genType{}, err
	}
	cols, err := collectColumns(st)
	if err != nil {
		return genType{}, err
	}
	if len(cols) == 0 {
		return genType{}, fmt.Errorf("struct has no Arrow columns")
	}

	exported := token.IsExported(name)
	gt := genType{
		GoName:       name,
		SchemaVar:    lowerFirst(name) + "ArrowSchema",
		SchemaFunc:   caseAs(exported, name+"Schema"),
		AppenderType: caseAs(exported, name+"Appender"),
		CtorName:     caseAs(exported, "New"+upperFirst(name)+"Appender"),
		BatchFunc:    caseAs(exported, name+"RecordBatch"),
	}
	for i, c := range cols {
		gt.needTime = gt.needTime || c.spec.needsTime
		gt.AnyFallible = gt.AnyFallible || c.spec.fallible
		gt.Fields = append(gt.Fields, genField{
			Index:       i,
			Name:        c.name,
			GoField:     c.goField,
			Nullable:    c.nullable(),
			ArrowType:   c.spec.arrowType,
			BuilderType: c.spec.builderType,
			BuilderVar:  fmt.Sprintf("b%d", i),
			AppendStmt:  renderAppend(i, c),
		})
	}
	return gt, nil
}

// claimIdents records the package-level names a type's generated code will
// declare, rejecting a second type that would declare the same one. Two types
// differing only in the case of their first letter collide this way; without
// this check the collision surfaces as a compile error in generated code the
// user did not write.
func claimIdents(declared map[string]string, name string, gt genType) error {
	for _, ident := range []string{gt.SchemaVar, gt.SchemaFunc, gt.AppenderType, gt.CtorName, gt.BatchFunc} {
		if prev, taken := declared[ident]; taken {
			return fmt.Errorf("generated name %s collides with the one generated for %s; generate the two types into separate files", ident, prev)
		}
		declared[ident] = name
	}
	return nil
}

func findStruct(pkg *packages.Package, name string) (*types.Struct, error) {
	obj := pkg.Types.Scope().Lookup(name)
	if obj == nil {
		return nil, fmt.Errorf("type not found in package %s", pkg.Name)
	}
	named, ok := obj.Type().(*types.Named)
	if !ok {
		return nil, fmt.Errorf("not a named type")
	}
	st, ok := named.Underlying().(*types.Struct)
	if !ok {
		return nil, fmt.Errorf("not a struct type")
	}
	return st, nil
}

// column is one resolved struct field.
type column struct {
	name     string
	goField  string
	ptrDepth int // pointer levels between the field and its value
	spec     colSpec
}

// nullable reports whether the column admits nulls, which for arreflect is a
// question about the field's outermost pointer and nothing else.
func (c column) nullable() bool { return c.ptrDepth > 0 }

// collectColumns walks the struct's fields in declaration order, which is the
// order arreflect settles on for a flat struct. Embedded fields are rejected
// rather than promoted: arreflect resolves promoted names by breadth and tag,
// and quietly reimplementing those rules is how a generator ends up emitting a
// different schema than the runtime it claims to match.
func collectColumns(st *types.Struct) ([]column, error) {
	var cols []column
	seen := make(map[string]string, st.NumFields())

	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if f.Anonymous() {
			return nil, fmt.Errorf("field %s: embedded fields are not supported; give the field a name or encode the struct with arreflect", f.Name())
		}
		if !f.Exported() {
			continue
		}

		tag, hasTag := reflect.StructTag(st.Tag(i)).Lookup("arrow")
		var opts tagOpts
		if hasTag {
			var err error
			if opts, err = parseTag(tag); err != nil {
				return nil, fmt.Errorf("field %s: %w", f.Name(), err)
			}
		}
		if opts.Skip {
			continue
		}
		if err := opts.validate(); err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name(), err)
		}

		name := opts.Name
		if name == "" {
			name = f.Name()
		}
		if prev, dup := seen[name]; dup {
			return nil, fmt.Errorf("fields %s and %s both map to column %q", prev, f.Name(), name)
		}
		seen[name] = f.Name()

		spec, ptrDepth, err := resolveColumn(f.Type(), opts)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name(), err)
		}
		cols = append(cols, column{name: name, goField: f.Name(), ptrDepth: ptrDepth, spec: spec})
	}
	return cols, nil
}

// renderAppend emits the append statement for one column, wrapping it in the
// null guards the reflection path applies: a nil pointer at any level is a
// null, and so is a nil []byte even when the column itself is not nullable.
func renderAppend(idx int, c column) string {
	bld := fmt.Sprintf("a.b%d", idx)
	val := strings.Repeat("*", c.ptrDepth) + "v." + c.goField
	recv := val
	if c.ptrDepth > 0 {
		// A method on the value has to bind to the value, not to a pointer.
		recv = "(" + val + ")"
	}

	stmt := c.spec.appendStmt(bld, recv, val)
	if c.spec.fallible {
		stmt = "a.setErr(" + stmt + ")"
	}

	// One nil check per pointer level, plus one on the value itself when a nil
	// value is a null in its own right.
	levels := c.ptrDepth
	if c.spec.nilable {
		levels++
	}
	if levels == 0 {
		return stmt
	}
	checks := make([]string, levels)
	for i := range checks {
		checks[i] = strings.Repeat("*", i) + "v." + c.goField + " == nil"
	}
	guard := strings.Join(checks, " || ")

	// A statement that is already a block (a time-of-day column scopes a local)
	// sheds its braces on the way into the guard's else, which would otherwise
	// nest two sets for no reason.
	inner := strings.TrimSuffix(strings.TrimPrefix(stmt, "{\n"), "\n}")
	return fmt.Sprintf("if %s {\n%s.AppendNull()\n} else {\n%s\n}", guard, bld, inner)
}

type genFile struct {
	Header   string
	Package  string
	NeedTime bool
	Types    []genType
}

type genType struct {
	GoName       string
	SchemaVar    string
	SchemaFunc   string
	AppenderType string
	CtorName     string
	BatchFunc    string
	Fields       []genField
	AnyFallible  bool

	needTime bool
}

type genField struct {
	Index       int
	Name        string
	GoField     string
	Nullable    bool
	ArrowType   string
	BuilderType string
	BuilderVar  string
	AppendStmt  string
}

func lowerFirst(s string) string { return mapFirst(s, unicode.ToLower) }
func upperFirst(s string) string { return mapFirst(s, unicode.ToUpper) }

func mapFirst(s string, f func(rune) rune) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = f(r[0])
	return string(r)
}

// caseAs gives a generated identifier the same visibility as the struct it was
// derived from, so generating for an unexported type does not silently widen
// the package's API.
func caseAs(exported bool, s string) string {
	if exported {
		return upperFirst(s)
	}
	return lowerFirst(s)
}

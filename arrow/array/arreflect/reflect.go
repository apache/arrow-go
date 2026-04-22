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

package arreflect

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	ErrUnsupportedType = errors.New("arreflect: unsupported type")
	ErrTypeMismatch    = errors.New("arreflect: type mismatch")
)

type tagOpts struct {
	Name             string
	Skip             bool
	Dict             bool
	View             bool
	REE              bool
	Large            bool
	DecimalPrecision int32
	DecimalScale     int32
	HasDecimalOpts   bool
	Temporal         string // "timestamp" (default), "date32", "date64", "time32", "time64"
	ParseErr         string // diagnostic set when decimal(p,s) tag fails to parse; surfaced by validateOptions
}

type fieldMeta struct {
	Name     string
	Index    []int
	Type     reflect.Type
	Nullable bool
	Opts     tagOpts
}

func parseTag(tag string) tagOpts {
	if tag == "-" {
		return tagOpts{Skip: true}
	}

	name, rest, _ := strings.Cut(tag, ",")
	opts := tagOpts{Name: name}

	if rest == "" {
		return opts
	}

	parseOptions(&opts, rest)
	return opts
}

func splitTagTokens(rest string) []string {
	var tokens []string
	depth := 0
	start := 0
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				tokens = append(tokens, strings.TrimSpace(rest[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(rest) {
		tokens = append(tokens, strings.TrimSpace(rest[start:]))
	}
	return tokens
}

func parseOptions(opts *tagOpts, rest string) {
	for _, token := range splitTagTokens(rest) {
		if strings.HasPrefix(token, "decimal(") && strings.HasSuffix(token, ")") {
			parseDecimalOpt(opts, token)
			continue
		}
		switch token {
		case "dict":
			opts.Dict = true
		case "view":
			opts.View = true
		case "ree":
			opts.REE = true
		case "large":
			opts.Large = true
		case "date32", "date64", "time32", "time64", "timestamp":
			opts.Temporal = token
		default:
			opts.ParseErr = fmt.Sprintf("unknown option %q", token)
		}
	}
}

func parseDecimalOpt(opts *tagOpts, token string) {
	inner := strings.TrimPrefix(token, "decimal(")
	inner = strings.TrimSuffix(inner, ")")
	parts := strings.SplitN(inner, ",", 2)
	if len(parts) != 2 {
		opts.ParseErr = fmt.Sprintf("invalid decimal tag %q: expected decimal(precision,scale)", token)
		return
	}
	p, errP := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
	if errP != nil {
		opts.ParseErr = fmt.Sprintf("invalid decimal tag %q: precision %q is not an integer", token, strings.TrimSpace(parts[0]))
		return
	}
	s, errS := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 32)
	if errS != nil {
		opts.ParseErr = fmt.Sprintf("invalid decimal tag %q: scale %q is not an integer", token, strings.TrimSpace(parts[1]))
		return
	}
	opts.HasDecimalOpts = true
	opts.DecimalPrecision = int32(p)
	opts.DecimalScale = int32(s)
}

type bfsEntry struct {
	t     reflect.Type
	index []int
	depth int
}

type candidate struct {
	meta   fieldMeta
	depth  int
	tagged bool
	order  int
}

type resolvedField struct {
	meta  fieldMeta
	order int
}

func collectFieldCandidates(t reflect.Type) map[string][]candidate {
	nameMap := make(map[string][]candidate)
	orderCounter := 0

	queue := []bfsEntry{{t: t, index: nil, depth: 0}}
	visited := make(map[reflect.Type]bool)

	for len(queue) > 0 {
		entry := queue[0]
		queue = queue[1:]

		st := entry.t
		for st.Kind() == reflect.Ptr {
			st = st.Elem()
		}
		if st.Kind() != reflect.Struct {
			continue
		}

		if visited[st] {
			continue
		}
		if entry.depth > 0 {
			visited[st] = true
		}

		for i := 0; i < st.NumField(); i++ {
			sf := st.Field(i)

			fullIndex := make([]int, len(entry.index)+1)
			copy(fullIndex, entry.index)
			fullIndex[len(entry.index)] = i

			if !sf.IsExported() && !sf.Anonymous {
				continue
			}

			tagVal, hasTag := sf.Tag.Lookup("arrow")
			var opts tagOpts
			if hasTag {
				opts = parseTag(tagVal)
			}

			if opts.Skip {
				continue
			}

			arrowName := opts.Name
			if arrowName == "" {
				arrowName = sf.Name
			}

			if sf.Anonymous && !hasTag {
				ft := sf.Type
				for ft.Kind() == reflect.Ptr {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					queue = append(queue, bfsEntry{
						t:     ft,
						index: fullIndex,
						depth: entry.depth + 1,
					})
					continue
				}
			}

			nullable := sf.Type.Kind() == reflect.Ptr
			tagged := hasTag && opts.Name != ""

			meta := fieldMeta{
				Name:     arrowName,
				Index:    fullIndex,
				Type:     sf.Type,
				Nullable: nullable,
				Opts:     opts,
			}

			existingCands := nameMap[arrowName]
			order := orderCounter
			if len(existingCands) > 0 {
				order = existingCands[0].order
			} else {
				orderCounter++
			}

			nameMap[arrowName] = append(existingCands, candidate{
				meta:   meta,
				depth:  entry.depth,
				tagged: tagged,
				order:  order,
			})
		}
	}

	return nameMap
}

func resolveFieldCandidates(nameMap map[string][]candidate) []fieldMeta {
	resolved := make([]resolvedField, 0, len(nameMap))
	for _, candidates := range nameMap {
		minDepth := candidates[0].depth
		for _, c := range candidates[1:] {
			if c.depth < minDepth {
				minDepth = c.depth
			}
		}

		var atMin []candidate
		for _, c := range candidates {
			if c.depth == minDepth {
				atMin = append(atMin, c)
			}
		}

		var winner *candidate
		if len(atMin) == 1 {
			winner = &atMin[0]
		} else {
			var tagged []candidate
			for _, c := range atMin {
				if c.tagged {
					tagged = append(tagged, c)
				}
			}
			if len(tagged) == 1 {
				winner = &tagged[0]
			}
		}

		if winner != nil {
			resolved = append(resolved, resolvedField{meta: winner.meta, order: winner.order})
		}
	}

	sort.Slice(resolved, func(i, j int) bool {
		return resolved[i].order < resolved[j].order
	})

	result := make([]fieldMeta, len(resolved))
	for i, r := range resolved {
		result[i] = r.meta
	}
	return result
}

func getStructFields(t reflect.Type) []fieldMeta {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil
	}

	return resolveFieldCandidates(collectFieldCandidates(t))
}

var structFieldCache sync.Map

func cachedStructFields(t reflect.Type) []fieldMeta {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if v, ok := structFieldCache.Load(t); ok {
		return v.([]fieldMeta)
	}

	fields := getStructFields(t)
	v, _ := structFieldCache.LoadOrStore(t, fields)
	return v.([]fieldMeta)
}

func fieldByIndexSafe(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, idx := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(idx)
	}
	return v, true
}

func At[T any](arr arrow.Array, i int) (T, error) {
	var result T
	v := reflect.ValueOf(&result).Elem()
	if err := setValue(v, arr, i); err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}

func ToSlice[T any](arr arrow.Array) ([]T, error) {
	n := arr.Len()
	result := make([]T, n)
	for i := 0; i < n; i++ {
		v := reflect.ValueOf(&result[i]).Elem()
		if err := setValue(v, arr, i); err != nil {
			return nil, fmt.Errorf("index %d: %w", i, err)
		}
	}
	return result, nil
}

// Option configures encoding behavior for [FromSlice] and [RecordFromSlice].
type Option func(*tagOpts)

// WithDict requests dictionary encoding for the top-level array.
func WithDict() Option { return func(o *tagOpts) { o.Dict = true } }

// WithView requests view-type encoding (STRING_VIEW, BINARY_VIEW, LIST_VIEW)
// for the top-level array and recursively for nested types.
func WithView() Option { return func(o *tagOpts) { o.View = true } }

// WithREE requests run-end encoding for the top-level array.
func WithREE() Option { return func(o *tagOpts) { o.REE = true } }

// WithDecimal sets the precision and scale for decimal types.
func WithDecimal(precision, scale int32) Option {
	return func(o *tagOpts) {
		o.DecimalPrecision = precision
		o.DecimalScale = scale
		o.HasDecimalOpts = true
	}
}

// WithTemporal overrides the Arrow temporal encoding for time.Time slices.
// Valid values: "date32", "date64", "time32", "time64", "timestamp" (default).
// Equivalent to tagging a struct field with arrow:",date32" etc.
// Invalid values cause FromSlice to return an error.
func WithTemporal(temporal string) Option {
	return func(o *tagOpts) { o.Temporal = temporal }
}

// WithLarge requests Large type variants (LARGE_STRING, LARGE_BINARY, LARGE_LIST,
// LARGE_LIST_VIEW) for the top-level array and recursively for nested types.
func WithLarge() Option { return func(o *tagOpts) { o.Large = true } }

func validateTemporalOpt(temporal string) error {
	switch temporal {
	case "", "timestamp", "date32", "date64", "time32", "time64":
		return nil
	default:
		return fmt.Errorf("arreflect: invalid WithTemporal value %q; valid values are date32, date64, time32, time64, timestamp: %w", temporal, ErrUnsupportedType)
	}
}

func validateOptions(opts tagOpts) error {
	if opts.ParseErr != "" {
		return fmt.Errorf("arreflect: %s: %w", opts.ParseErr, ErrUnsupportedType)
	}
	n := 0
	if opts.Dict {
		n++
	}
	if opts.REE {
		n++
	}
	if opts.View {
		n++
	}
	if n > 1 {
		return fmt.Errorf("arreflect: conflicting options: only one of WithDict, WithREE, WithView may be specified: %w", ErrUnsupportedType)
	}
	return nil
}

func buildEmptyTyped(goType reflect.Type, opts tagOpts, mem memory.Allocator) (arrow.Array, error) {
	dt, err := inferArrowType(goType)
	if err != nil {
		return nil, err
	}
	derefType := goType
	for derefType.Kind() == reflect.Ptr {
		derefType = derefType.Elem()
	}
	dt = applyDecimalOpts(dt, derefType, opts)
	dt = applyTemporalOpts(dt, derefType, opts)
	if opts.Large {
		if !hasLargeableType(dt) {
			return nil, fmt.Errorf("arreflect: large option has no effect on type %s: %w", dt, ErrUnsupportedType)
		}
		dt = applyLargeOpts(dt)
	}
	if opts.View {
		if derefType.Kind() == reflect.Slice && derefType != typeOfByteSlice {
			// slice-of-slices: build a LIST_VIEW or LARGE_LIST_VIEW
			innerElem := derefType.Elem()
			for innerElem.Kind() == reflect.Ptr {
				innerElem = innerElem.Elem()
			}
			innerDT, err := inferArrowType(innerElem)
			if err != nil {
				return nil, err
			}
			innerDT = applyViewOpts(innerDT)
			if opts.Large {
				dt = arrow.LargeListViewOf(innerDT)
			} else {
				dt = arrow.ListViewOf(innerDT)
			}
		} else {
			// primitive/string/binary: apply view recursively
			if !hasViewableType(dt) {
				return nil, fmt.Errorf("arreflect: view option has no effect on type %s: %w", dt, ErrUnsupportedType)
			}
			dt = applyViewOpts(dt)
		}
	}
	if opts.Dict {
		if err := validateDictValueType(dt); err != nil {
			return nil, err
		}
		dt = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: dt}
	} else if opts.REE {
		dt = arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, dt)
	}
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	return b.NewArray(), nil
}

func FromSlice[T any](vals []T, mem memory.Allocator, opts ...Option) (arrow.Array, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	var tOpts tagOpts
	for _, o := range opts {
		o(&tOpts)
	}
	if err := validateOptions(tOpts); err != nil {
		return nil, err
	}
	if err := validateTemporalOpt(tOpts.Temporal); err != nil {
		return nil, err
	}
	if tOpts.Temporal != "" {
		goType := reflect.TypeFor[T]()
		deref := goType
		for deref.Kind() == reflect.Ptr {
			deref = deref.Elem()
		}
		if deref != typeOfTime {
			return nil, fmt.Errorf("arreflect: WithTemporal requires a time.Time element type, got %s: %w", deref, ErrUnsupportedType)
		}
	}
	if len(vals) == 0 {
		return buildEmptyTyped(reflect.TypeFor[T](), tOpts, mem)
	}
	sv := reflect.ValueOf(vals)
	return buildArray(sv, tOpts, mem)
}

func RecordToSlice[T any](rec arrow.RecordBatch) ([]T, error) {
	sa := array.RecordToStructArray(rec)
	defer sa.Release()
	return ToSlice[T](sa)
}

func RecordFromSlice[T any](vals []T, mem memory.Allocator, opts ...Option) (arrow.RecordBatch, error) {
	arr, err := FromSlice[T](vals, mem, opts...)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	sa, ok := arr.(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("arreflect: RecordFromSlice requires a struct type T, got %v", reflect.TypeFor[T]())
	}
	return array.RecordFromStructArray(sa, nil), nil
}

// RecordAt converts the row at index i of an Arrow Record to a Go value of type T.
// T must be a struct type whose fields correspond to the record's columns.
func RecordAt[T any](rec arrow.RecordBatch, i int) (T, error) {
	sa := array.RecordToStructArray(rec)
	defer sa.Release()
	return At[T](sa, i)
}

// RecordAtAny converts the row at index i of an Arrow Record to a Go value,
// inferring the Go type from the record's schema at runtime via [InferGoType].
// Equivalent to AtAny on the struct array underlying the record.
func RecordAtAny(rec arrow.RecordBatch, i int) (any, error) {
	sa := array.RecordToStructArray(rec)
	defer sa.Release()
	return AtAny(sa, i)
}

// RecordToAnySlice converts all rows of an Arrow Record to Go values,
// inferring the Go type at runtime via [InferGoType].
// Equivalent to ToAnySlice on the struct array underlying the record.
func RecordToAnySlice(rec arrow.RecordBatch) ([]any, error) {
	sa := array.RecordToStructArray(rec)
	defer sa.Release()
	return ToAnySlice(sa)
}

// AtAny converts a single element at index i of an Arrow array to a Go value,
// inferring the Go type from the Arrow DataType at runtime via [InferGoType].
// Useful when the column type is not known at compile time.
// Null elements are returned as the Go zero value of the inferred type; use
// arr.IsNull(i) to distinguish a null element from a genuine zero.
// For typed access when T is known, prefer [At].
func AtAny(arr arrow.Array, i int) (any, error) {
	goType, err := InferGoType(arr.DataType())
	if err != nil {
		return nil, err
	}
	result := reflect.New(goType).Elem()
	if err := setValue(result, arr, i); err != nil {
		return nil, err
	}
	return result.Interface(), nil
}

// ToAnySlice converts all elements of an Arrow array to Go values,
// inferring the Go type from the Arrow DataType at runtime via [InferGoType].
// All elements share the same inferred Go type. Null elements are returned as
// the Go zero value of the inferred type; use arr.IsNull(i) to distinguish
// a null element from a genuine zero value.
// For typed access when T is known, prefer [ToSlice].
func ToAnySlice(arr arrow.Array) ([]any, error) {
	goType, err := InferGoType(arr.DataType())
	if err != nil {
		return nil, err
	}
	n := arr.Len()
	result := make([]any, n)
	for i := 0; i < n; i++ {
		v := reflect.New(goType).Elem()
		if err := setValue(v, arr, i); err != nil {
			return nil, fmt.Errorf("index %d: %w", i, err)
		}
		result[i] = v.Interface()
	}
	return result, nil
}

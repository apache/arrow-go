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
	ListView         bool
	REE              bool
	DecimalPrecision int32
	DecimalScale     int32
	HasDecimalOpts   bool
	Temporal         string // "timestamp" (default), "date32", "date64", "time32", "time64"
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

	var name, rest string
	if idx := strings.Index(tag, ","); idx >= 0 {
		name = tag[:idx]
		rest = tag[idx+1:]
	} else {
		name = tag
		rest = ""
	}

	opts := tagOpts{Name: name}

	if rest == "" {
		return opts
	}

	parseOptions(&opts, rest)
	return opts
}

func parseOptions(opts *tagOpts, rest string) {
	for len(rest) > 0 {
		var token string
		if idx := strings.Index(rest, ","); idx >= 0 {
			token = rest[:idx]
			rest = rest[idx+1:]
		} else {
			token = rest
			rest = ""
		}
		token = strings.TrimSpace(token)

		if strings.HasPrefix(token, "decimal(") {
			if strings.HasSuffix(token, ")") {
				parseDecimalOpt(opts, token)
				continue
			}
			next := token
			for len(rest) > 0 {
				var part string
				if idx := strings.Index(rest, ","); idx >= 0 {
					part = rest[:idx]
					rest = rest[idx+1:]
				} else {
					part = rest
					rest = ""
				}
				next = next + "," + strings.TrimSpace(part)
				if strings.HasSuffix(next, ")") {
					break
				}
			}
			parseDecimalOpt(opts, next)
			continue
		}

		switch token {
		case "dict":
			opts.Dict = true
		case "listview":
			opts.ListView = true
		case "ree":
			opts.REE = true
		case "date32", "date64", "time32", "time64", "timestamp":
			opts.Temporal = token
		}
	}
}

func parseDecimalOpt(opts *tagOpts, token string) {
	inner := strings.TrimPrefix(token, "decimal(")
	inner = strings.TrimSuffix(inner, ")")
	parts := strings.SplitN(inner, ",", 2)
	if len(parts) == 2 {
		p, errP := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
		s, errS := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 32)
		if errP == nil && errS == nil {
			opts.HasDecimalOpts = true
			opts.DecimalPrecision = int32(p)
			opts.DecimalScale = int32(s)
		}
	}
}

func getStructFields(t reflect.Type) []fieldMeta {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil
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

			// Assign insertion order on first encounter of this name.
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

	type resolvedField struct {
		meta  fieldMeta
		order int
	}

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

	for i := 1; i < len(resolved); i++ {
		for j := i; j > 0 && resolved[j].order < resolved[j-1].order; j-- {
			resolved[j], resolved[j-1] = resolved[j-1], resolved[j]
		}
	}

	result := make([]fieldMeta, len(resolved))
	for i, r := range resolved {
		result[i] = r.meta
	}
	return result
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

func Get[T any](arr arrow.Array, i int) (T, error) {
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

func FromSlice[T any](vals []T, mem memory.Allocator) (arrow.Array, error) {
	if len(vals) == 0 {
		dt, err := inferArrowType(reflect.TypeFor[T]())
		if err != nil {
			return nil, err
		}
		b := array.NewBuilder(mem, dt)
		defer b.Release()
		return b.NewArray(), nil
	}
	sv := reflect.ValueOf(vals)
	return buildArray(sv, tagOpts{}, mem)
}

func FromSliceDefault[T any](vals []T) (arrow.Array, error) {
	return FromSlice(vals, memory.DefaultAllocator)
}

func RecordToSlice[T any](rec arrow.Record) ([]T, error) {
	sa := array.RecordToStructArray(rec)
	defer sa.Release()
	return ToSlice[T](sa)
}

func RecordFromSlice[T any](vals []T, mem memory.Allocator) (arrow.Record, error) {
	arr, err := FromSlice[T](vals, mem)
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

func RecordFromSliceDefault[T any](vals []T) (arrow.Record, error) {
	return RecordFromSlice(vals, memory.DefaultAllocator)
}

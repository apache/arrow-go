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

package compute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func checkStartsWith(t *testing.T, ctx context.Context, opts compute.MatchSubstringOptions, input, expected compute.Datum) {
	t.Helper()
	got, err := compute.StartsWith(ctx, opts, input)
	require.NoError(t, err)
	defer got.Release()
	assertDatumsEqual(t, expected, got, nil, nil)
}

func TestStartsWith(t *testing.T) {
	newCtx := func(t *testing.T) (context.Context, memory.Allocator) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		return compute.WithAllocator(context.Background(), mem), mem
	}
	fromJSON := func(t *testing.T, mem memory.Allocator, dt arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, dt, strings.NewReader(json))
		require.NoError(t, err)
		return arr
	}

	tests := []struct {
		name, pattern, input, expected string
	}{
		{"mixed", "ab", `["", "a", "ab", "abc", "ba", "xabc", null]`, `[false, false, true, true, false, false, null]`},
		{"empty pattern", "", `["", "a", null]`, `[true, true, null]`},
		{"all null", "ab", `[null, null]`, `[null, null]`},
		{"empty", "ab", `[]`, `[]`},
		{"long pattern", "abcdef",
			`["abcdefghijklm", "abcdeXghijklm", "abcdefghijkl", "Xbcdefghijklm"]`,
			`[true, false, true, false]`},
		{"long values", "ab", `["abcdefghijklm", "aXcdefghijklm"]`, `[true, false]`},
		{"pattern longer than value", "abcdefghijklmn", `["abcdefghijklm", "nopqrstuvwxyz"]`, `[false, false]`},
	}

	for _, dt := range []arrow.DataType{
		arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString,
		arrow.BinaryTypes.BinaryView, arrow.BinaryTypes.StringView,
	} {
		for _, tt := range tests {
			t.Run(dt.String()+"/"+tt.name, func(t *testing.T) {
				ctx, mem := newCtx(t)

				strs := fromJSON(t, mem, arrow.BinaryTypes.String, tt.input)
				defer strs.Release()
				input, err := compute.CastArray(ctx, strs, compute.SafeCastOptions(dt))
				require.NoError(t, err)
				defer input.Release()
				expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, tt.expected)
				defer expected.Release()

				in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
				checkScalarUnary(t, "starts_with", in, exp, &compute.MatchSubstringOptions{Pattern: tt.pattern})
			})
		}
	}

	t.Run("non-utf8 binary", func(t *testing.T) {
		ctx, mem := newCtx(t)
		input := fromJSON(t, mem, arrow.BinaryTypes.Binary, `["//5h", "/w==", "Yf8=", ""]`)
		defer input.Release()
		expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, true, false, false]`)
		defer expected.Release()

		checkStartsWith(t, ctx, compute.MatchSubstringOptions{Pattern: "\xff"},
			compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
	})

	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.StringView} {
		t.Run("sliced/"+dt.String(), func(t *testing.T) {
			ctx, mem := newCtx(t)
			full := fromJSON(t, mem, dt,
				`["ab", null, "x", "abc", null, "ab", "b", "abd", "a", "ab"]`)
			defer full.Release()
			input := array.NewSlice(full, 3, 9)
			defer input.Release()
			expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, null, true, false, true, false]`)
			defer expected.Release()

			checkStartsWith(t, ctx, compute.MatchSubstringOptions{Pattern: "ab"},
				compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
		})
	}

	t.Run("multi-buffer view", func(t *testing.T) {
		ctx, mem := newCtx(t)
		long := strings.Repeat("x", 20000)
		bldr := array.NewBinaryViewBuilder(mem)
		defer bldr.Release()
		for _, v := range []string{"abcdef" + long, "abcdef" + long, "abcdXf" + long, "abcdef", "abcdef" + long} {
			bldr.Append([]byte(v))
		}
		bldr.AppendNull()
		full := bldr.NewArray()
		defer full.Release()
		require.Greater(t, len(full.Data().Buffers()), 3)
		input := array.NewSlice(full, 1, 6)
		defer input.Release()
		expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, false, true, true, null]`)
		defer expected.Release()

		checkStartsWith(t, ctx, compute.MatchSubstringOptions{Pattern: "abcdef"},
			compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
	})

	t.Run("chunked", func(t *testing.T) {
		ctx, mem := newCtx(t)
		var chunks []arrow.Array
		for _, json := range []string{`["ab", null]`, `[]`, `["xab", "abc"]`} {
			c := fromJSON(t, mem, arrow.BinaryTypes.String, json)
			defer c.Release()
			chunks = append(chunks, c)
		}
		input := arrow.NewChunked(arrow.BinaryTypes.String, chunks)
		defer input.Release()
		want := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, null, false, true]`)
		defer want.Release()
		expected := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{want})
		defer expected.Release()

		checkStartsWith(t, ctx, compute.MatchSubstringOptions{Pattern: "ab"},
			compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
	})

	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.StringView} {
		t.Run("dictionary/"+dt.String(), func(t *testing.T) {
			ctx, mem := newCtx(t)
			dict := fromJSON(t, mem, dt, `["ab", null, "xab"]`)
			defer dict.Release()
			indices := fromJSON(t, mem, arrow.PrimitiveTypes.Int8, `[0, 1, 2, null, 0]`)
			defer indices.Release()
			input := array.NewDictionaryArray(&arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int8, ValueType: dt}, indices, dict)
			defer input.Release()
			expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, null, false, null, true]`)
			defer expected.Release()

			checkStartsWith(t, ctx, compute.MatchSubstringOptions{Pattern: "ab"},
				compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
		})
	}

	t.Run("ignore case", func(t *testing.T) {
		strs := []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.StringView}
		bins := []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary, arrow.BinaryTypes.BinaryView}
		tests := []struct {
			name, pattern string
			types         []arrow.DataType
			input         []string
			expected      string
		}{
			{"multibyte folds", "\u212a\u017f", strs, []string{"ks", "KSx", "\u212a\u017f", "k"}, `[true, true, true, false]`},
			{"no full folding", "\u00df", strs, []string{"SS", "\u1e9e", "\u00df"}, `[false, true, true]`},
			{"no special casing", "i", strs, []string{"I", "\u0130", "\u0131"}, `[true, false, false]`},
			{"invalid input", "\ufffd", strs, []string{"\ufffd", "\xff", "\xffbcdefghijklm"}, `[true, false, false]`},
			{"fold changes length", "abcdefghijk\u212a", strs,
				[]string{"abcdefghijkxyz", "abcdefghijkK", "ABCDEFGHIJKkxyz", "abcdefghijk\u212a\u212a", "abcdefghijkx"},
				`[false, true, true, true, false]`},
			{"ascii folds only", "\xe0a", bins,
				[]string{"\xe0A", "\xc0a", "\xe0", "\xc0abcdefghijklm", "\xe0Abcdefghijklm"},
				`[true, false, false, false, true]`},
			{"empty pattern", "", append(strs, bins...), []string{"", "\xff"}, `[true, true]`},
		}

		for _, tt := range tests {
			for _, dt := range tt.types {
				t.Run(tt.name+"/"+dt.String(), func(t *testing.T) {
					_, mem := newCtx(t)
					bldr := array.NewBuilder(mem, dt)
					defer bldr.Release()
					bldr.(interface{ AppendStringValues([]string, []bool) }).AppendStringValues(tt.input, nil)
					input := bldr.NewArray()
					defer input.Release()
					expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, tt.expected)
					defer expected.Release()

					in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
					opts := compute.MatchSubstringOptions{Pattern: tt.pattern, IgnoreCase: true}
					checkScalarUnary(t, "starts_with", in, exp, &opts)
				})
			}
		}

		for _, dt := range []arrow.DataType{arrow.BinaryTypes.StringView, arrow.BinaryTypes.BinaryView} {
			t.Run("sliced/"+dt.String(), func(t *testing.T) {
				ctx, mem := newCtx(t)
				bldr := array.NewBuilder(mem, dt).(interface {
					array.Builder
					AppendString(string)
				})
				defer bldr.Release()
				bldr.AppendString("abc")
				bldr.AppendString("ABcdefghijklmn")
				bldr.AppendNull()
				for _, v := range []string{"aBc", "abXdefghijklmn", "xbcdefghijklmn"} {
					bldr.AppendString(v)
				}
				full := bldr.NewArray()
				defer full.Release()
				input := array.NewSlice(full, 1, 6)
				defer input.Release()
				expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, null, true, false, false]`)
				defer expected.Release()

				checkStartsWith(t, ctx, compute.MatchSubstringOptions{Pattern: "abc", IgnoreCase: true},
					compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected))
			})
		}
	})

	t.Run("options", func(t *testing.T) {
		fn, ok := compute.GetFunctionRegistry().GetFunction("starts_with")
		require.True(t, ok)
		assert.NoError(t, fn.Validate())

		ctx, mem := newCtx(t)
		arr := fromJSON(t, mem, arrow.BinaryTypes.String, `["ab", "b"]`)
		defer arr.Release()
		input := compute.NewDatumWithoutOwning(arr)
		expected := fromJSON(t, mem, arrow.FixedWidthTypes.Boolean, `[true, false]`)
		defer expected.Release()

		got, err := compute.CallFunction(ctx, "starts_with", compute.MatchSubstringOptions{Pattern: "a"}, input)
		require.NoError(t, err)
		defer got.Release()
		assertDatumsEqual(t, compute.NewDatumWithoutOwning(expected), got, nil, nil)

		_, err = compute.CallFunction(ctx, "starts_with", nil, input)
		assert.ErrorIs(t, err, arrow.ErrInvalid)
		_, err = compute.CallFunction(ctx, "starts_with", (*compute.MatchSubstringOptions)(nil), input)
		assert.ErrorIs(t, err, arrow.ErrInvalid)
		_, err = compute.CallFunction(ctx, "starts_with", &compute.ArithmeticOptions{}, input)
		assert.ErrorIs(t, err, arrow.ErrInvalid)
		_, err = compute.StartsWith(ctx, compute.MatchSubstringOptions{Pattern: "\xff", IgnoreCase: true}, input)
		assert.ErrorIs(t, err, arrow.ErrInvalid)
		sv := fromJSON(t, mem, arrow.BinaryTypes.StringView, `["ab"]`)
		defer sv.Release()
		_, err = compute.StartsWith(ctx, compute.MatchSubstringOptions{Pattern: "\xff", IgnoreCase: true},
			compute.NewDatumWithoutOwning(sv))
		assert.ErrorIs(t, err, arrow.ErrInvalid)
	})
}

func TestUtf8Length(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	ctx := compute.WithAllocator(context.Background(), mem)
	fromJSON := func(dt arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, dt, strings.NewReader(json))
		require.NoError(t, err)
		t.Cleanup(arr.Release)
		return arr
	}

	tests := []struct {
		name, input, expected string
	}{
		{"ascii", `["", "a", "abc"]`, `[0, 1, 3]`},
		{"multibyte", `["", "ábc", "日本語", "🦄", "aá日🦄", null]`, `[0, 3, 3, 1, 4, null]`},
		{"all null", `[null, null]`, `[null, null]`},
		{"empty array", `[]`, `[]`},
	}

	for _, dt := range []arrow.DataType{
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.StringView,
	} {
		for _, tt := range tests {
			t.Run(dt.String()+"/"+tt.name, func(t *testing.T) {
				strs := fromJSON(arrow.BinaryTypes.String, tt.input)
				input, err := compute.CastArray(ctx, strs, compute.SafeCastOptions(dt))
				require.NoError(t, err)
				defer input.Release()

				outType := arrow.PrimitiveTypes.Int32
				if dt.ID() == arrow.LARGE_STRING {
					outType = arrow.PrimitiveTypes.Int64
				}
				expected := fromJSON(outType, tt.expected)

				in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
				defer in.Release()
				defer exp.Release()
				checkScalarUnary(t, "utf8_length", in, exp, nil)
			})
		}
	}

	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.StringView} {
		t.Run("sliced/"+dt.String(), func(t *testing.T) {
			full := fromJSON(dt, `["ab", null, "x", "abc", null, "ab", "b", "abd", "a", "ab"]`)
			input := array.NewSlice(full, 3, 9)
			defer input.Release()
			expected := fromJSON(arrow.PrimitiveTypes.Int32, `[3, null, 2, 1, 3, 1]`)

			in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
			defer in.Release()
			defer exp.Release()
			checkScalarUnary(t, "utf8_length", in, exp, nil)
		})
	}

	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.StringView} {
		t.Run("dictionary/"+dt.String(), func(t *testing.T) {
			dict := fromJSON(dt, `["ab", null, "xab"]`)
			indices := fromJSON(arrow.PrimitiveTypes.Int8, `[0, 1, 2, null, 0]`)
			input := array.NewDictionaryArray(&arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int8, ValueType: dt}, indices, dict)
			defer input.Release()
			expected := fromJSON(arrow.PrimitiveTypes.Int32, `[2, null, 3, null, 2]`)

			in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
			defer in.Release()
			defer exp.Release()
			checkScalarUnary(t, "utf8_length", in, exp, nil)
		})
	}

	t.Run("invalid utf8 counted by code point headers", func(t *testing.T) {
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()
		for _, v := range []string{"\xff\x80", "\xff", "\xc3\x28"} {
			bldr.Append(v)
		}
		input := bldr.NewArray()
		defer input.Release()
		expected := fromJSON(arrow.PrimitiveTypes.Int32, `[1, 1, 2]`)

		in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
		defer in.Release()
		defer exp.Release()
		checkScalarUnary(t, "utf8_length", in, exp, nil)
	})
}

func TestUtf8CaseConversion(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	ctx := compute.WithAllocator(context.Background(), mem)
	fromJSON := func(dt arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, dt, strings.NewReader(json))
		require.NoError(t, err)
		t.Cleanup(arr.Release)
		return arr
	}

	tests := []struct {
		name, input, upper, lower string
	}{
		{"ascii", `["", "abc", "ABC", "aBc"]`, `["", "ABC", "ABC", "ABC"]`, `["", "abc", "abc", "abc"]`},
		{"latin1", `["ábc", "ÁBC"]`, `["ÁBC", "ÁBC"]`, `["ábc", "ábc"]`},
		{"greek", `["ω", "Ω", "ς"]`, `["Ω", "Ω", "Σ"]`, `["ω", "ω", "ς"]`},
		{"cyrillic", `["д", "Д"]`, `["Д", "Д"]`, `["д", "д"]`},
		{"turkish dotted i and eszett", `["ß", "ı", "İ"]`, `["ß", "I", "İ"]`, `["ß", "ı", "i"]`},
		{"unchanged", `["日本語", "🦄", "42"]`, `["日本語", "🦄", "42"]`, `["日本語", "🦄", "42"]`},
		{"nulls", `["ab", null, "AB"]`, `["AB", null, "AB"]`, `["ab", null, "ab"]`},
		{"all null", `[null, null]`, `[null, null]`, `[null, null]`},
		{"empty array", `[]`, `[]`, `[]`},
	}

	for _, dt := range []arrow.DataType{
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.StringView,
	} {
		for _, tt := range tests {
			for _, fn := range []struct{ name, expected string }{
				{"utf8_upper", tt.upper}, {"utf8_lower", tt.lower},
			} {
				t.Run(dt.String()+"/"+fn.name+"/"+tt.name, func(t *testing.T) {
					strs := fromJSON(arrow.BinaryTypes.String, tt.input)
					input, err := compute.CastArray(ctx, strs, compute.SafeCastOptions(dt))
					require.NoError(t, err)
					defer input.Release()

					expected := fromJSON(arrow.BinaryTypes.String, fn.expected)
					want, err := compute.CastArray(ctx, expected, compute.SafeCastOptions(dt))
					require.NoError(t, err)
					defer want.Release()

					in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(want)
					defer in.Release()
					defer exp.Release()
					checkScalarUnary(t, fn.name, in, exp, nil)
				})
			}
		}
	}

	for _, dt := range []arrow.DataType{
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.StringView,
	} {
		t.Run("sliced/"+dt.String(), func(t *testing.T) {
			full := fromJSON(dt, `["ab", null, "x", "AbC", null, "aB", "b", "abD", "a", "ab"]`)
			input := array.NewSlice(full, 3, 9)
			defer input.Release()
			expected := fromJSON(dt, `["ABC", null, "AB", "B", "ABD", "A"]`)

			in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
			defer in.Release()
			defer exp.Release()
			checkScalarUnary(t, "utf8_upper", in, exp, nil)
		})
	}

	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.StringView} {
		t.Run("dictionary/"+dt.String(), func(t *testing.T) {
			dict := fromJSON(dt, `["ab", null, "xAB"]`)
			indices := fromJSON(arrow.PrimitiveTypes.Int8, `[0, 1, 2, null, 0]`)
			input := array.NewDictionaryArray(&arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int8, ValueType: dt}, indices, dict)
			defer input.Release()
			expected := fromJSON(dt, `["AB", null, "XAB", null, "AB"]`)

			in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
			defer in.Release()
			defer exp.Release()
			checkScalarUnary(t, "utf8_upper", in, exp, nil)
		})
	}

	t.Run("multi-buffer view", func(t *testing.T) {
		long := strings.Repeat("x", 20000)
		upperLong := strings.Repeat("X", 20000)
		bldr := array.NewStringViewBuilder(mem)
		defer bldr.Release()
		for _, v := range []string{"abcdef" + long, "abcdef" + long, "abcdXf" + long, "abcdef", "abcdef" + long} {
			bldr.Append(v)
		}
		bldr.AppendNull()
		full := bldr.NewArray()
		defer full.Release()
		require.Greater(t, len(full.Data().Buffers()), 3)
		input := array.NewSlice(full, 1, 6)
		defer input.Release()

		expBldr := array.NewStringViewBuilder(mem)
		defer expBldr.Release()
		expBldr.Append("ABCDEF" + upperLong)
		expBldr.Append("ABCDXF" + upperLong)
		expBldr.Append("ABCDEF")
		expBldr.Append("ABCDEF" + upperLong)
		expBldr.AppendNull()
		expected := expBldr.NewArray()
		defer expected.Release()

		in, exp := compute.NewDatumWithoutOwning(input), compute.NewDatumWithoutOwning(expected)
		defer in.Release()
		defer exp.Release()
		checkScalarUnary(t, "utf8_upper", in, exp, nil)
	})

	for _, dt := range []arrow.DataType{
		arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.StringView,
	} {
		t.Run("invalid utf8/"+dt.String(), func(t *testing.T) {
			bldr := array.NewBuilder(mem, dt)
			defer bldr.Release()
			type appender interface{ AppendString(string) }
			ap := bldr.(appender)
			ap.AppendString("\xffbcdefghijklm")
			ap.AppendString("ab\xff")
			input := bldr.NewArray()
			defer input.Release()

			d := compute.NewDatumWithoutOwning(input)
			defer d.Release()
			_, err := compute.CallFunction(ctx, "utf8_upper", nil, d)
			assert.ErrorIs(t, err, arrow.ErrInvalid)
			_, err = compute.CallFunction(ctx, "utf8_lower", nil, d)
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestUtf8StringFuncs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	ctx := compute.WithAllocator(context.Background(), mem)

	input, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["aBá", null]`))
	require.NoError(t, err)
	defer input.Release()
	in := compute.NewDatumWithoutOwning(input)
	defer in.Release()

	lengths, err := compute.Utf8Length(ctx, in)
	require.NoError(t, err)
	defer lengths.Release()
	expLen, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[3, null]`))
	require.NoError(t, err)
	defer expLen.Release()
	assertDatumsEqual(t, compute.NewDatumWithoutOwning(expLen), lengths, nil, nil)

	upper, err := compute.Utf8Upper(ctx, in)
	require.NoError(t, err)
	defer upper.Release()
	expUpper, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["ABÁ", null]`))
	require.NoError(t, err)
	defer expUpper.Release()
	assertDatumsEqual(t, compute.NewDatumWithoutOwning(expUpper), upper, nil, nil)

	lower, err := compute.Utf8Lower(ctx, in)
	require.NoError(t, err)
	defer lower.Release()
	expLower, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["abá", null]`))
	require.NoError(t, err)
	defer expLower.Release()
	assertDatumsEqual(t, compute.NewDatumWithoutOwning(expLower), lower, nil, nil)
}

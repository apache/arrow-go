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

package variant

import (
	"fmt"
	"testing"
)

func benchmarkNewWithMetadata(b *testing.B, value Value) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := NewWithMetadata(value.Metadata(), value.Bytes()); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkObjectValue(b *testing.B, numFields int) Value {
	b.Helper()

	var builder Builder
	start := builder.Offset()
	fields := make([]FieldEntry, 0, numFields)
	for i := range numFields {
		key := fmt.Sprintf("field%02d", i)
		fields = append(fields, builder.NextField(start, key))
		if err := builder.AppendInt(int64(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := builder.FinishObject(start, fields); err != nil {
		b.Fatal(err)
	}

	value, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	return value
}

func benchmarkArrayValue(b *testing.B, numElements int) Value {
	b.Helper()

	var builder Builder
	start := builder.Offset()
	offsets := make([]int, 0, numElements)
	for i := range numElements {
		offsets = append(offsets, builder.NextElement(start))
		if err := builder.AppendInt(int64(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := builder.FinishArray(start, offsets); err != nil {
		b.Fatal(err)
	}

	value, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	return value
}

func BenchmarkNewWithMetadataObject40Fields(b *testing.B) {
	benchmarkNewWithMetadata(b, benchmarkObjectValue(b, 40))
}

func BenchmarkNewWithMetadataArray40Elements(b *testing.B) {
	benchmarkNewWithMetadata(b, benchmarkArrayValue(b, 40))
}

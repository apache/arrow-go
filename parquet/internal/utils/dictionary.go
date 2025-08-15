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

package utils

import (
	"math"
	"reflect"

	"github.com/apache/arrow-go/v18/parquet"
)

// IndexType is the type we're going to use for Dictionary indexes, currently
// an alias to int32
type IndexType = int32

// Max and Min constants for the IndexType
const (
	MaxIndexType = math.MaxInt32
	MinIndexType = math.MinInt32
)

// DictionaryConverter is an interface used for dealing with RLE decoding and encoding
// when working with dictionaries to get values from indexes.
type DictionaryConverter interface {
	// Copy takes an interface{} which must be a slice of the appropriate type, and will be populated
	// by the dictionary values at the indexes from the IndexType slice
	Copy(interface{}, []IndexType) error
	// Fill fills interface{} which must be a slice of the appropriate type, with the value
	// specified by the dictionary index passed in.
	Fill(interface{}, IndexType) error
	// FillZero fills interface{}, which must be a slice of the appropriate type, with the zero value
	// for the given type.
	FillZero(interface{})
	// IsValid validates that all of the indexes passed in are valid indexes for the dictionary
	IsValid(...IndexType) bool
}

type DictionaryConverter2[T parquet.ColumnTypes | uint64] interface {
	// Copy populates the input slice by the dictionary values at the indexes from the IndexType slice
	Copy2([]T, []IndexType) error
	// Fill fills the input slice with the value specified by the dictionary index passed in.
	Fill2([]T, IndexType) error
	// FillZero fills the input slice with the zero value for the given type.
	FillZero2([]T)
	// IsValid validates that all of the indexes passed in are valid indexes for the dictionary
	IsValid(...IndexType) bool
	// IsValidSingle(IndexType) bool
}

// converter for getspaced that handles runs that get returned directly
// as output, rather than using a dictionary
type plainConverter[T ~uint64] struct{}

func (plainConverter[T]) IsValid(...IndexType) bool { return true }

func (plainConverter[T]) Fill(values interface{}, val IndexType) error {
	v := reflect.ValueOf(values)
	switch v.Type().Elem().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.Index(0).SetInt(int64(val))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.Index(0).SetUint(uint64(val))
	}

	for i := 1; i < v.Len(); i *= 2 {
		reflect.Copy(v.Slice(i, v.Len()), v.Slice(0, i))
	}
	return nil
}

func (plainConverter[T]) FillZero(values interface{}) {
	v := reflect.ValueOf(values)
	zeroVal := reflect.New(v.Type().Elem()).Elem()

	v.Index(0).Set(zeroVal)
	for i := 1; i < v.Len(); i *= 2 {
		reflect.Copy(v.Slice(i, v.Len()), v.Slice(0, i))
	}
}

func (plainConverter[T]) Copy(out interface{}, values []IndexType) error {
	vout := reflect.ValueOf(out)
	vin := reflect.ValueOf(values)
	for i := 0; i < vin.Len(); i++ {
		vout.Index(i).Set(vin.Index(i).Convert(vout.Type().Elem()))
	}
	return nil
}

func (plainConverter[T]) Fill2(values []T, val IndexType) error {
	if len(values) == 0 {
		return nil
	}
	values[0] = T(val)
	for i := 1; i < len(values); i *= 2 {
		copy(values[i:], values[0:i])
	}
	return nil
}

func (plainConverter[T]) FillZero2(values []T) {
	if len(values) == 0 {
		return
	}
	var zero T
	values[0] = zero
	for i := 1; i < len(values); i *= 2 {
		copy(values[i:], values[0:i])
	}
}

func (plainConverter[T]) Copy2(out []T, values []IndexType) error {
	for i := range values {
		out[i] = T(values[i])
	}
	return nil
}

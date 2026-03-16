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

package geoarrow

import (
	"encoding/json"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// a generic geometry array that can be used with all geoarrow types
type geometryArray[V GeometryValue, G GeometryType[V]] struct {
	array.ExtensionArrayBase
}

func (a *geometryArray[V, G]) String() string {
	o := new(strings.Builder)
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		o.WriteString(a.ValueStr(i))
	}
	return o.String()
}

func (a *geometryArray[V, G]) Value(i int) V {
	geomType := a.ExtensionType().(G)
	return geomType.valueFromArray(a, i)
}

func (a *geometryArray[V, G]) Values() []V {
	values := make([]V, a.Len())
	for i := range values {
		values[i] = a.Value(i)
	}
	return values
}

func (a *geometryArray[V, G]) ValueStr(i int) string {
	if a.IsNull(i) {
		return array.NullValueStr
	}
	return a.Value(i).String()
}

func (a *geometryArray[V, G]) GetOneForMarshal(i int) any {
	if a.IsValid(i) {
		return a.Value(i)
	}
	return nil
}

func (a *geometryArray[V, G]) MarshalJSON() ([]byte, error) {
	vals := make([]any, a.Len())
	for i := range vals {
		vals[i] = a.GetOneForMarshal(i)
	}
	return json.Marshal(vals)
}

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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowjson "github.com/apache/arrow-go/v18/internal/json"
)

const (
	ExtensionNameWKB     = "geoarrow.wkb"
	ExtensionNamePoint   = "geoarrow.point"
	ExtensionNamePolygon = "geoarrow.polygon"
	// TODO ExtensionNameWKT = "geoarrow.wkt"
	// TODO ExtensionNameLineString = "geoarrow.linestring"
	// TODO ExtensionNameMultiPoint = "geoarrow.multipoint"
	// TODO ExtensionNameMultiLineString = "geoarrow.multilinestring"
	// TODO ExtensionNameMultiPolygon = "geoarrow.multipolygon"
	// TODO ExtensionNameGeometry = "geoarrow.geometry"
	// TODO ExtensionNameGeometryCollection = "geoarrow.geometrycollection"
	// TODO ExtensionNameBox = "geoarrow.box"
)

// GeometryType represents a GeoArrow geometry type with arbitrary
// format and encoding.
type GeometryType[V GeometryValue] interface {
	arrow.ExtensionType
	valueFromArray(a array.ExtensionArray, i int) V
	appendValueToBuilder(b array.Builder, v V)
	valueFromString(s string) (V, error)
	unmarshalJSONOne(dec *arrowjson.Decoder) (V, bool, error)
}

// Extension is a base struct that can be embedded in a GeoArrow extension
//
//	type to provide common metadata and serialization logic.
type Extension struct {
	meta Metadata
}

// Metadata returns the GeoArrow metadata associated with this extension type.
func (e *Extension) Metadata() Metadata {
	return e.meta
}

// Serialize returns the JSON string representation of the GeoArrow metadata for
// this extension type.
func (e *Extension) Serialize() string {
	if e == nil {
		return ""
	}

	// Ignore errors since Metadata fields are well-defined and should always
	// serialize successfully
	serialized, _ := json.Marshal(e.meta)
	return string(serialized)
}

// Equal compares two Extension instances for equality based on their metadata.
func (e *Extension) Equal(other *Extension) bool {
	if e == nil && other == nil {
		return true
	}
	if e == nil || other == nil {
		return false
	}
	if e.meta.CRSType != other.meta.CRSType {
		return false
	}
	if e.meta.Edges != other.meta.Edges {
		return false
	}

	if len(e.meta.CRS) != len(other.meta.CRS) {
		return false
	}
	for i, r := range e.meta.CRS {
		if r != other.meta.CRS[i] {
			return false
		}
	}

	return true
}

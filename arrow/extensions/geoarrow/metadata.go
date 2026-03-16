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
)

// Dimensions represents the coordinate layout of the geometry data
type Dimensions int

const (
	DimensionEmpty Dimensions = iota
	DimensionXY
	DimensionXYZ
	DimensionXYM
	DimensionXYZM
)

// EdgeInterpolation represents the edge interpolation method for the geometry data
type EdgeInterpolation string

const (
	// EdgePlanar indicates that edges should be interpreted as straight lines in a Cartesian plane
	EdgePlanar EdgeInterpolation = ""
	// EdgeSpherical indicates that edges should be interpreted as great circle arcs on a sphere
	EdgeSpherical EdgeInterpolation = "spherical"
	// Below are intpolations methods defined on the ellipsoid specified in the CRS.
	// EdgeVincenty indicates that edges should be interpreted using Vincenty's formula.
	EdgeVincenty EdgeInterpolation = "vincenty"
	// EdgeThomas indicates that edges should be interpreted using Thomas' formula.
	EdgeThomas EdgeInterpolation = "thomas"
	// EdgeAndoyer indicates that edges should be interpreted using Andoyer's formula.
	EdgeAndoyer EdgeInterpolation = "andoyer"
	// EdgeKarney indicates that edges should be interpreted using Karney's formula.
	EdgeKarney EdgeInterpolation = "karney"
)

// CRSType represents the type of coordinate reference system (CRS) used in the geometry metadata
type CRSType string

const (
	// CRS formatted in PROJJSON format
	// https://proj.org/specifications/projjson.html
	CRSTypePROJJSON CRSType = "projjson"
	// CRS formatted in WKT2:2019 format
	// https://www.ogc.org/publications/standard/wkt-crs/
	CRSTypeWKT22019 CRSType = "wkt2:2019"
	// CRS formatted in AUTHORITY:CODE format, e.g. "EPSG:4326"
	CRSTypeAuthorityCode CRSType = "authority_code"
	// CRS as an opaque identifier
	CRSTypeSRID CRSType = "srid"
)

type Metadata struct {
	// CRS as a PROJJSON object or a string in the format specified by CRSType
	CRS     json.RawMessage   `json:"crs,omitempty"`
	CRSType CRSType           `json:"crs_type,omitempty"`
	Edges   EdgeInterpolation `json:"edges,omitempty"`
}

// NewMetadata creates a new Metadata instance with default values (empty CRS and planar edges)
func NewMetadata() Metadata {
	return Metadata{}
}

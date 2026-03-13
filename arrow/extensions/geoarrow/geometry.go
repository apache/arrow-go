package geoarrow

import (
	"encoding/json"
	"fmt"
)

type CoordinateDimension int

const (
	XY CoordinateDimension = iota
	XYZ
	XYM
	XYZM
)

func (d CoordinateDimension) NDim() int {
	switch d {
	case XY:
		return 2
	case XYZ, XYM:
		return 3
	case XYZM:
		return 4
	default:
		return 0
	}
}

type Layout int

const (
	Separate Layout = iota
	Interleaved
)

// GeometryTypeID represents the specific geometry type (e.g., Point,
// LineString, Polygon) and its coordinate dimension (e.g., XY, XYZ, XYM, XYZM)
type GeometryTypeID int

// As defined in https://github.com/geoarrow/geoarrow
const (
	PointID               GeometryTypeID = 1
	LineStringID          GeometryTypeID = 2
	PolygonID             GeometryTypeID = 3
	MultiPointID          GeometryTypeID = 4
	MultiLineStringID     GeometryTypeID = 5
	MultiPolygonID        GeometryTypeID = 6
	PointZID              GeometryTypeID = 11
	LineStringZID         GeometryTypeID = 12
	PolygonZID            GeometryTypeID = 13
	MultiPointZID         GeometryTypeID = 14
	MultiLineStringZID    GeometryTypeID = 15
	MultiPolygonZID       GeometryTypeID = 16
	PointMID              GeometryTypeID = 21
	LineStringMID         GeometryTypeID = 22
	PolygonMID            GeometryTypeID = 23
	MultiPointMIDID       GeometryTypeID = 24
	MultiLineStringMIDID  GeometryTypeID = 25
	MultiPolygonMIDID     GeometryTypeID = 26
	PointZMID             GeometryTypeID = 31
	LineStringZMID        GeometryTypeID = 32
	PolygonZMID           GeometryTypeID = 33
	MultiPointZMIDID      GeometryTypeID = 34
	MultiLineStringZMIDID GeometryTypeID = 35
	MultiPolygonZMIDID    GeometryTypeID = 36
)

// GeometryValue represents a single concrete value of a GeoArrow geometry
type GeometryValue interface {
	fmt.Stringer
	json.Marshaler
	IsEmpty() bool
	Dimension() CoordinateDimension
	GeometryType() GeometryTypeID
}

package geoarrow

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowjson "github.com/apache/arrow-go/v18/internal/json"
)

type PointType struct {
	arrow.ExtensionBase
	Extension
}

type PointValue struct {
	coords []float64
	dim    CoordinateDimension
}

func NewPointValue(x, y float64) PointValue {
	return PointValue{coords: []float64{x, y}, dim: XY}
}

func NewPointValueZ(x, y, z float64) PointValue {
	return PointValue{coords: []float64{x, y, z}, dim: XYZ}
}

func NewPointValueM(x, y, m float64) PointValue {
	return PointValue{coords: []float64{x, y, m}, dim: XYM}
}

func NewPointValueZM(x, y, z, m float64) PointValue {
	return PointValue{coords: []float64{x, y, z, m}, dim: XYZM}
}

func (v PointValue) X() float64 {
	return v.coords[0]
}

func (v PointValue) Y() float64 {
	return v.coords[1]
}

func (v PointValue) Z() float64 {
	if v.dim != XYZ && v.dim != XYZM {
		return 0
	}
	return v.coords[2]
}

func (v PointValue) M() float64 {
	if v.dim != XYM && v.dim != XYZM {
		return 0
	}
	return v.coords[len(v.coords)-1]
}

func (v PointValue) Dimension() CoordinateDimension {
	return v.dim
}

func (v PointValue) GeometryType() GeometryTypeID {
	switch v.dim {
	case XY:
		return PointID
	case XYZ:
		return PointZID
	case XYM:
		return PointMID
	case XYZM:
		return PointZMID
	default:
		panic("invalid coordinate dimension for PointValue")
	}
}

func (v PointValue) Coordinates() []float64 {
	return v.coords
}

func (v PointValue) String() string {
	b := strings.Builder{}
	b.WriteString("POINT")
	switch v.dim {
	case XYZ:
		b.WriteString(" Z")
	case XYM:
		b.WriteString(" M")
	case XYZM:
		b.WriteString(" ZM")
	}
	b.WriteString("(")
	for i, coord := range v.coords {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(strconv.FormatFloat(coord, 'f', 6, 64))
	}
	b.WriteString(")")
	return b.String()
}

func (v PointValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.coords)
}

func (v PointValue) IsEmpty() bool {
	return len(v.coords) == 0
}

func NewPointType(opts ...pointOption) *PointType {
	pt := &PointType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		)},
		Extension: Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(pt)
		}
	}
	return pt
}

type pointOption func(*PointType)

// PointWithCRS configures the PointType with a specific CRS in the metadata
func PointWithCRS(crs json.RawMessage, crsType CRSType) pointOption {
	return func(pt *PointType) {
		pt.Extension.meta.CRS = crs
		pt.Extension.meta.CRSType = crsType
	}
}

func PointWithStorage(storage arrow.DataType) pointOption {
	return func(pt *PointType) {
		pt.Storage = storage
	}
}

func PointWithMetadata(metadata Metadata) pointOption {
	return func(pt *PointType) {
		pt.Extension.meta = metadata
	}
}

func (pt *PointType) ExtensionName() string {
	return ExtensionNamePoint
}

func (pt *PointType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}

	return NewPointType(PointWithStorage(storageType), PointWithMetadata(meta)), nil
}

func (pt *PointType) ExtensionEquals(other arrow.ExtensionType) bool {
	otherPt, ok := other.(*PointType)
	if !ok {
		return false
	}

	if pt == nil && otherPt == nil {
		return true
	}
	if pt == nil || otherPt == nil {
		return false
	}

	return pt.Storage.ID() == otherPt.Storage.ID() && pt.Extension.Equal(&otherPt.Extension)
}

func (pt *PointType) ArrayType() reflect.Type {
	return reflect.TypeOf(PointArray{})
}

// dimensionFromStructType determines the coordinate dimension from an Arrow struct type's fields.
func dimensionFromStructType(st *arrow.StructType) CoordinateDimension {
	switch st.NumFields() {
	case 2:
		return XY
	case 3:
		if st.Field(2).Name == "z" {
			return XYZ
		}
		return XYM
	case 4:
		return XYZM
	default:
		return XY
	}
}

func dimensionFromFieldCount(_ int, structArr *array.Struct) CoordinateDimension {
	return dimensionFromStructType(structArr.DataType().(*arrow.StructType))
}

func (pt *PointType) valueFromArray(a array.ExtensionArray, i int) PointValue {
	if a.IsNull(i) {
		return PointValue{}
	}

	structArr := a.Storage().(*array.Struct)
	nFields := structArr.NumField()
	coords := make([]float64, nFields)
	for j := 0; j < nFields; j++ {
		coords[j] = structArr.Field(j).(*array.Float64).Value(i)
	}

	return PointValue{coords: coords, dim: dimensionFromFieldCount(nFields, structArr)}
}

func (pt *PointType) appendValueToBuilder(b array.Builder, v PointValue) {
	sb := b.(*array.StructBuilder)
	sb.Append(true)
	for j, coord := range v.coords {
		sb.FieldBuilder(j).(*array.Float64Builder).Append(coord)
	}
}

func (pt *PointType) valueFromString(s string) (PointValue, error) {
	// Parse WKT-style: "POINT(1.0 2.0)" or "POINT Z(1.0 2.0 3.0)" etc.
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(strings.ToUpper(s), "POINT") {
		return PointValue{}, fmt.Errorf("invalid point WKT: %s", s)
	}

	// Find the opening paren
	openParen := strings.Index(s, "(")
	if openParen == -1 {
		return PointValue{}, fmt.Errorf("invalid point WKT: missing '(': %s", s)
	}
	closeParen := strings.Index(s, ")")
	if closeParen == -1 {
		return PointValue{}, fmt.Errorf("invalid point WKT: missing ')': %s", s)
	}

	coordStr := strings.TrimSpace(s[openParen+1 : closeParen])
	parts := strings.Fields(coordStr)
	coords := make([]float64, len(parts))
	for i, part := range parts {
		f, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return PointValue{}, fmt.Errorf("invalid coordinate in WKT: %s", part)
		}
		coords[i] = f
	}

	var dim CoordinateDimension
	switch len(coords) {
	case 2:
		dim = XY
	case 3:
		// Check prefix for Z vs M
		prefix := strings.ToUpper(s[:openParen])
		if strings.Contains(prefix, "M") && !strings.Contains(prefix, "ZM") {
			dim = XYM
		} else {
			dim = XYZ
		}
	case 4:
		dim = XYZM
	default:
		return PointValue{}, fmt.Errorf("invalid number of coordinates: %d", len(coords))
	}

	return PointValue{coords: coords, dim: dim}, nil
}

func (pt *PointType) unmarshalJSONOne(dec *arrowjson.Decoder) (PointValue, bool, error) {
	t, err := dec.Token()
	if err != nil {
		return PointValue{}, false, err
	}

	if t == nil {
		return PointValue{}, true, nil
	}

	// Point JSON is an array of coordinates: [1.0, 2.0]
	delim, ok := t.(arrowjson.Delim)
	if !ok || delim != '[' {
		return PointValue{}, false, fmt.Errorf("expected '[' for Point value, got %T(%v)", t, t)
	}

	var coords []float64
	for dec.More() {
		var f float64
		if err := dec.Decode(&f); err != nil {
			return PointValue{}, false, err
		}
		coords = append(coords, f)
	}
	// consume closing ']'
	if _, err := dec.Token(); err != nil {
		return PointValue{}, false, err
	}

	// Determine dimension from the storage type's struct fields
	storage := pt.StorageType().(*arrow.StructType)
	nFields := storage.NumFields()
	var dim CoordinateDimension
	switch nFields {
	case 2:
		dim = XY
	case 3:
		if storage.Field(2).Name == "z" {
			dim = XYZ
		} else {
			dim = XYM
		}
	case 4:
		dim = XYZM
	}

	return PointValue{coords: coords, dim: dim}, false, nil
}

func (pt *PointType) NewBuilder(mem memory.Allocator) array.Builder {
	return &Builder[PointValue, *PointType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, pt),
	}
}

type PointArray = geometryArray[PointValue, *PointType]

var _ array.CustomExtensionBuilder = (*PointType)(nil)

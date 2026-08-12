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

package extensions

import (
	"bytes"
	"encoding/binary"
	stdjson "encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/internal/json"
)

const BigDecimalLayoutVersion int8 = 1

// BigDecimalClass identifies the numeric category and sign stored in the
// first byte of an encoded BigDecimal value.
type BigDecimalClass uint8

const (
	BigDecimalNegativeSignalingNaN BigDecimalClass = iota
	BigDecimalNegativeQuietNaN
	BigDecimalNegativeInfinity
	BigDecimalNegativeFinite
	BigDecimalPositiveFinite
	BigDecimalPositiveInfinity
	BigDecimalPositiveQuietNaN
	BigDecimalPositiveSignalingNaN
)

func (c BigDecimalClass) valid() bool { return c <= BigDecimalPositiveSignalingNaN }

// IsFinite reports whether c represents a finite value.
func (c BigDecimalClass) IsFinite() bool {
	return c == BigDecimalNegativeFinite || c == BigDecimalPositiveFinite
}

// IsInfinity reports whether c represents an infinity.
func (c BigDecimalClass) IsInfinity() bool {
	return c == BigDecimalNegativeInfinity || c == BigDecimalPositiveInfinity
}

// IsNaN reports whether c represents a quiet or signaling NaN.
func (c BigDecimalClass) IsNaN() bool {
	return c == BigDecimalNegativeSignalingNaN || c == BigDecimalNegativeQuietNaN ||
		c == BigDecimalPositiveQuietNaN || c == BigDecimalPositiveSignalingNaN
}

// IsNegative reports whether c represents a negative value.
func (c BigDecimalClass) IsNegative() bool { return c <= BigDecimalNegativeFinite }

// BigDecimal is a logical BigDecimal value. The magnitude is an unsigned,
// minimally encoded little-endian integer. Scale is meaningful only for
// finite values. Its Go zero value represents -sNaN, not numeric zero, because
// the wire layout assigns class byte 0x00 to negative signaling NaN.
type BigDecimal struct {
	class     BigDecimalClass
	scale     int16
	magnitude []byte
}

// NewFiniteBigDecimal constructs a finite BigDecimal from an absolute
// magnitude, a scale, and a sign. A negative zero is preserved when negative
// is true and magnitude is zero.
func NewFiniteBigDecimal(magnitude *big.Int, scale int16, negative bool) (BigDecimal, error) {
	if magnitude == nil {
		return BigDecimal{}, fmt.Errorf("%w: BigDecimal magnitude must not be nil", arrow.ErrInvalid)
	}
	if magnitude.Sign() < 0 {
		return BigDecimal{}, fmt.Errorf("%w: BigDecimal magnitude must be unsigned", arrow.ErrInvalid)
	}

	class := BigDecimalPositiveFinite
	if negative {
		class = BigDecimalNegativeFinite
	}

	return BigDecimal{
		class:     class,
		scale:     scale,
		magnitude: bigIntToLittleEndian(magnitude),
	}, nil
}

// NewBigDecimalFromBigInt constructs a finite BigDecimal from a signed
// unscaled integer and a scale.
func NewBigDecimalFromBigInt(unscaled *big.Int, scale int16) (BigDecimal, error) {
	if unscaled == nil {
		return BigDecimal{}, fmt.Errorf("%w: BigDecimal unscaled integer must not be nil", arrow.ErrInvalid)
	}

	magnitude := new(big.Int).Abs(unscaled)
	return NewFiniteBigDecimal(magnitude, scale, unscaled.Sign() < 0)
}

// NewNonFiniteBigDecimal constructs an infinity or NaN value.
func NewNonFiniteBigDecimal(class BigDecimalClass) (BigDecimal, error) {
	if !class.valid() || class.IsFinite() {
		return BigDecimal{}, fmt.Errorf("%w: class 0x%02x is not a non-finite BigDecimal class", arrow.ErrInvalid, uint8(class))
	}
	return BigDecimal{class: class}, nil
}

func bigIntToLittleEndian(v *big.Int) []byte {
	if v.Sign() == 0 {
		return []byte{0}
	}

	out := v.Bytes()
	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out
}

func littleEndianToBigInt(v []byte) *big.Int {
	buf := make([]byte, len(v))
	for i := range v {
		buf[len(v)-1-i] = v[i]
	}
	return new(big.Int).SetBytes(buf)
}

// Class returns the value's numeric category and sign.
func (v BigDecimal) Class() BigDecimalClass { return v.class }

// Scale returns the scale for a finite value. Non-finite values always have
// scale zero.
func (v BigDecimal) Scale() int16 { return v.scale }

// Magnitude returns a new big.Int containing the absolute unscaled integer.
// It returns nil for non-finite values.
func (v BigDecimal) Magnitude() *big.Int {
	if !v.class.IsFinite() {
		return nil
	}
	return littleEndianToBigInt(v.magnitude)
}

// IsFinite reports whether v is a finite value.
func (v BigDecimal) IsFinite() bool { return v.class.IsFinite() }

// IsInfinity reports whether v is an infinity.
func (v BigDecimal) IsInfinity() bool { return v.class.IsInfinity() }

// IsNaN reports whether v is a quiet or signaling NaN.
func (v BigDecimal) IsNaN() bool { return v.class.IsNaN() }

// IsNegative reports whether v carries a negative sign.
func (v BigDecimal) IsNegative() bool { return v.class.IsNegative() }

func (v BigDecimal) String() string {
	switch v.class {
	case BigDecimalNegativeSignalingNaN:
		return "-sNaN"
	case BigDecimalNegativeQuietNaN:
		return "-NaN"
	case BigDecimalNegativeInfinity:
		return "-Infinity"
	case BigDecimalPositiveInfinity:
		return "Infinity"
	case BigDecimalPositiveQuietNaN:
		return "NaN"
	case BigDecimalPositiveSignalingNaN:
		return "sNaN"
	}

	digits := v.Magnitude().String()
	scale := int(v.scale)
	var out string
	switch {
	case scale == 0:
		out = digits
	case scale < 0:
		out = digits + "E" + strconv.Itoa(-scale)
	case scale < len(digits):
		point := len(digits) - scale
		out = digits[:point] + "." + digits[point:]
	default:
		out = "0." + strings.Repeat("0", scale-len(digits)) + digits
	}

	if v.IsNegative() {
		return "-" + out
	}
	return out
}

// ParseBigDecimal parses the canonical finite or non-finite spelling produced
// by BigDecimal.String. Scientific notation represents a negative scale
// without expanding the coefficient.
func ParseBigDecimal(s string) (BigDecimal, error) {
	switch s {
	case "-sNaN":
		return NewNonFiniteBigDecimal(BigDecimalNegativeSignalingNaN)
	case "-NaN":
		return NewNonFiniteBigDecimal(BigDecimalNegativeQuietNaN)
	case "-Infinity":
		return NewNonFiniteBigDecimal(BigDecimalNegativeInfinity)
	case "Infinity":
		return NewNonFiniteBigDecimal(BigDecimalPositiveInfinity)
	case "NaN":
		return NewNonFiniteBigDecimal(BigDecimalPositiveQuietNaN)
	case "sNaN":
		return NewNonFiniteBigDecimal(BigDecimalPositiveSignalingNaN)
	}

	original := s
	if s == "" {
		return BigDecimal{}, fmt.Errorf("%w: empty BigDecimal string", arrow.ErrInvalid)
	}

	negative := false
	if s[0] == '-' {
		negative = true
		s = s[1:]
		if s == "" {
			return BigDecimal{}, fmt.Errorf("%w: BigDecimal string contains only a sign", arrow.ErrInvalid)
		}
	}

	exponent := int64(0)
	if idx := strings.IndexAny(s, "eE"); idx >= 0 {
		if strings.ContainsAny(s[idx+1:], "eE") {
			return BigDecimal{}, fmt.Errorf("%w: invalid BigDecimal exponent", arrow.ErrInvalid)
		}
		var err error
		exponent, err = strconv.ParseInt(s[idx+1:], 10, 64)
		if err != nil {
			return BigDecimal{}, fmt.Errorf("%w: invalid BigDecimal exponent: %v", arrow.ErrInvalid, err)
		}
		s = s[:idx]
	}

	integer, fraction := s, ""
	if before, after, ok0 := strings.Cut(s, "."); ok0 {
		if strings.IndexByte(after, '.') >= 0 {
			return BigDecimal{}, fmt.Errorf("%w: BigDecimal contains multiple decimal points", arrow.ErrInvalid)
		}
		integer, fraction = before, after
	}
	if integer == "" && fraction == "" {
		return BigDecimal{}, fmt.Errorf("%w: BigDecimal has no digits", arrow.ErrInvalid)
	}
	for _, part := range []string{integer, fraction} {
		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return BigDecimal{}, fmt.Errorf("%w: invalid BigDecimal digit %q", arrow.ErrInvalid, ch)
			}
		}
	}

	scale := int64(len(fraction)) - exponent
	if scale < math.MinInt16 || scale > math.MaxInt16 {
		return BigDecimal{}, fmt.Errorf("%w: BigDecimal scale %d is outside the int16 range", arrow.ErrInvalid, scale)
	}

	coefficient := integer + fraction
	if coefficient == "" {
		coefficient = "0"
	}
	magnitude, ok := new(big.Int).SetString(coefficient, 10)
	if !ok {
		return BigDecimal{}, fmt.Errorf("%w: invalid BigDecimal coefficient", arrow.ErrInvalid)
	}
	value, err := NewFiniteBigDecimal(magnitude, int16(scale), negative)
	if err != nil {
		return BigDecimal{}, err
	}
	if value.String() != original {
		return BigDecimal{}, fmt.Errorf("%w: non-canonical BigDecimal string %q", arrow.ErrInvalid, original)
	}
	return value, nil
}

// ValidateBigDecimalEncoding validates the layout-level invariants for a
// packed BigDecimal value.
func ValidateBigDecimalEncoding(encoded []byte) error {
	if len(encoded) < 3 {
		return fmt.Errorf("%w: BigDecimal value must contain at least three bytes", arrow.ErrInvalid)
	}

	class := BigDecimalClass(encoded[0])
	if !class.valid() {
		return fmt.Errorf("%w: invalid BigDecimal class 0x%02x", arrow.ErrInvalid, encoded[0])
	}
	scale := int16(binary.LittleEndian.Uint16(encoded[1:3]))

	if !class.IsFinite() {
		if scale != 0 {
			return fmt.Errorf("%w: non-finite BigDecimal values must have scale zero", arrow.ErrInvalid)
		}
		if len(encoded) != 3 {
			return fmt.Errorf("%w: non-finite BigDecimal values must not have a magnitude", arrow.ErrInvalid)
		}
		return nil
	}

	magnitude := encoded[3:]
	if len(magnitude) == 0 {
		return fmt.Errorf("%w: finite BigDecimal values must have a magnitude", arrow.ErrInvalid)
	}
	if len(magnitude) > 1 && magnitude[len(magnitude)-1] == 0 {
		return fmt.Errorf("%w: BigDecimal magnitude is not minimally encoded", arrow.ErrInvalid)
	}
	return nil
}

// DecodeBigDecimal decodes a validated packed BigDecimal value.
func DecodeBigDecimal(encoded []byte) (BigDecimal, error) {
	if err := ValidateBigDecimalEncoding(encoded); err != nil {
		return BigDecimal{}, err
	}

	class := BigDecimalClass(encoded[0])
	value := BigDecimal{
		class: class,
		scale: int16(binary.LittleEndian.Uint16(encoded[1:3])),
	}
	if class.IsFinite() {
		value.magnitude = append([]byte(nil), encoded[3:]...)
	}
	return value, nil
}

// EncodeBigDecimal encodes value using the packed BigDecimal layout.
func EncodeBigDecimal(value BigDecimal) ([]byte, error) {
	if !value.class.valid() {
		return nil, fmt.Errorf("%w: invalid BigDecimal class 0x%02x", arrow.ErrInvalid, uint8(value.class))
	}

	if !value.class.IsFinite() {
		if value.scale != 0 || len(value.magnitude) != 0 {
			return nil, fmt.Errorf("%w: non-finite BigDecimal values cannot have scale or magnitude", arrow.ErrInvalid)
		}
		return []byte{byte(value.class), 0, 0}, nil
	}

	if len(value.magnitude) == 0 {
		return nil, fmt.Errorf("%w: finite BigDecimal values must have a magnitude", arrow.ErrInvalid)
	}
	if len(value.magnitude) > 1 && value.magnitude[len(value.magnitude)-1] == 0 {
		return nil, fmt.Errorf("%w: BigDecimal magnitude is not minimally encoded", arrow.ErrInvalid)
	}

	out := make([]byte, 3+len(value.magnitude))
	out[0] = byte(value.class)
	binary.LittleEndian.PutUint16(out[1:3], uint16(value.scale))
	copy(out[3:], value.magnitude)
	return out, nil
}

// BigDecimalMetadata contains optional informational schema-level hints. A
// zero LayoutVersion is normalized to BigDecimalLayoutVersion. Metadata is
// part of type identity but is not used to accept or reject row values.
type BigDecimalMetadata struct {
	LayoutVersion    int8
	MaxPrecision     *int32
	MaxScale         *int16
	SupportsInfinity *bool
	SupportsNaN      *bool
}

func (m BigDecimalMetadata) clone() BigDecimalMetadata {
	out := m
	if m.MaxPrecision != nil {
		v := *m.MaxPrecision
		out.MaxPrecision = &v
	}
	if m.MaxScale != nil {
		v := *m.MaxScale
		out.MaxScale = &v
	}
	if m.SupportsInfinity != nil {
		v := *m.SupportsInfinity
		out.SupportsInfinity = &v
	}
	if m.SupportsNaN != nil {
		v := *m.SupportsNaN
		out.SupportsNaN = &v
	}
	return out
}

func normalizeBigDecimalMetadata(metadata BigDecimalMetadata) (BigDecimalMetadata, error) {
	metadata = metadata.clone()
	if metadata.LayoutVersion == 0 {
		metadata.LayoutVersion = BigDecimalLayoutVersion
	}
	if metadata.LayoutVersion != BigDecimalLayoutVersion {
		return BigDecimalMetadata{}, fmt.Errorf("%w: unsupported BigDecimal layout version %d", arrow.ErrInvalid, metadata.LayoutVersion)
	}
	if metadata.MaxPrecision != nil && *metadata.MaxPrecision <= 0 {
		return BigDecimalMetadata{}, fmt.Errorf("%w: BigDecimal max_precision must be positive", arrow.ErrInvalid)
	}
	return metadata, nil
}

// Metadata is type-identity wire data, so it deliberately uses encoding/json
// for a stable representation. Array JSON uses internal/json like other arrays.
type bigDecimalMetadataJSON struct {
	LayoutVersion    *int8  `json:"layout_version,omitempty"`
	MaxPrecision     *int32 `json:"max_precision,omitempty"`
	MaxScale         *int16 `json:"max_scale,omitempty"`
	SupportsInfinity *bool  `json:"supports_infinity,omitempty"`
	SupportsNaN      *bool  `json:"supports_nan,omitempty"`
}

// BigDecimalType represents arbitrary-precision decimal values stored using
// the packed BigDecimal layout in a BinaryView array.
type BigDecimalType struct {
	arrow.ExtensionBase
	metadata BigDecimalMetadata
}

// NewBigDecimalType returns a version-one BigDecimal type without optional
// metadata hints.
func NewBigDecimalType() *BigDecimalType {
	typ, err := NewBigDecimalTypeWithMetadata(BigDecimalMetadata{})
	if err != nil {
		panic(err)
	}
	return typ
}

// NewBigDecimalTypeWithMetadata returns a BigDecimal type with informational
// schema-level metadata.
func NewBigDecimalTypeWithMetadata(metadata BigDecimalMetadata) (*BigDecimalType, error) {
	metadata, err := normalizeBigDecimalMetadata(metadata)
	if err != nil {
		return nil, err
	}
	return &BigDecimalType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.BinaryTypes.BinaryView},
		metadata:      metadata,
	}, nil
}

// Metadata returns a defensive copy of the type metadata.
func (b *BigDecimalType) Metadata() BigDecimalMetadata { return b.metadata.clone() }

func (*BigDecimalType) ArrayType() reflect.Type { return reflect.TypeOf(BigDecimalArray{}) }

func (*BigDecimalType) ExtensionName() string { return "arrow.big_decimal" }

func (b *BigDecimalType) ExtensionEquals(other arrow.ExtensionType) bool {
	if other == nil || b.ExtensionName() != other.ExtensionName() {
		return false
	}
	rhs, ok := other.(*BigDecimalType)
	if !ok {
		return false
	}
	return arrow.TypeEqual(b.Storage, rhs.Storage) &&
		b.metadata.LayoutVersion == rhs.metadata.LayoutVersion &&
		equalOptional(b.metadata.MaxPrecision, rhs.metadata.MaxPrecision) &&
		equalOptional(b.metadata.MaxScale, rhs.metadata.MaxScale) &&
		equalOptional(b.metadata.SupportsInfinity, rhs.metadata.SupportsInfinity) &&
		equalOptional(b.metadata.SupportsNaN, rhs.metadata.SupportsNaN)
}

func equalOptional[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (b *BigDecimalType) Serialize() string {
	if b.metadata.MaxPrecision == nil && b.metadata.MaxScale == nil &&
		b.metadata.SupportsInfinity == nil && b.metadata.SupportsNaN == nil {
		return ""
	}

	wire := bigDecimalMetadataJSON{
		MaxPrecision:     b.metadata.MaxPrecision,
		MaxScale:         b.metadata.MaxScale,
		SupportsInfinity: b.metadata.SupportsInfinity,
		SupportsNaN:      b.metadata.SupportsNaN,
	}
	data, err := stdjson.Marshal(wire)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func (*BigDecimalType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if !arrow.TypeEqual(storageType, arrow.BinaryTypes.BinaryView) {
		return nil, fmt.Errorf("%w: invalid storage type for BigDecimalType: %s", arrow.ErrInvalid, storageType)
	}

	data = strings.TrimSpace(data)
	if data == "" {
		return NewBigDecimalType(), nil
	}
	if data == "null" {
		return nil, fmt.Errorf("%w: BigDecimal metadata must be a JSON object", arrow.ErrInvalid)
	}

	var wire bigDecimalMetadataJSON
	dec := stdjson.NewDecoder(strings.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil {
		return nil, fmt.Errorf("%w: invalid BigDecimal metadata: %v", arrow.ErrInvalid, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing data in BigDecimal metadata", arrow.ErrInvalid)
	}

	metadata := BigDecimalMetadata{
		MaxPrecision:     wire.MaxPrecision,
		MaxScale:         wire.MaxScale,
		SupportsInfinity: wire.SupportsInfinity,
		SupportsNaN:      wire.SupportsNaN,
	}
	if wire.LayoutVersion != nil {
		// An explicit JSON zero is invalid; only an absent version defaults to one.
		if *wire.LayoutVersion != BigDecimalLayoutVersion {
			return nil, fmt.Errorf("%w: unsupported BigDecimal layout version %d",
				arrow.ErrInvalid, *wire.LayoutVersion)
		}
		metadata.LayoutVersion = *wire.LayoutVersion
	}
	return NewBigDecimalTypeWithMetadata(metadata)
}

func (b *BigDecimalType) String() string {
	if metadata := b.Serialize(); metadata != "" {
		return fmt.Sprintf("extension<%s[metadata=%s]>", b.ExtensionName(), metadata)
	}
	return fmt.Sprintf("extension<%s>", b.ExtensionName())
}

// Fingerprint distinguishes BigDecimal metadata because metadata is part of
// this extension type's identity.
func (b *BigDecimalType) Fingerprint() string {
	metadata := b.Serialize()
	return fmt.Sprintf("%s%d:%s%d:%s", b.ExtensionBase.Fingerprint(), len(b.ExtensionName()),
		b.ExtensionName(), len(metadata), metadata)
}

func (b *BigDecimalType) NewBuilder(mem memory.Allocator) array.Builder {
	return NewBigDecimalBuilder(mem, b)
}

// BigDecimalArray exposes packed BinaryView values as logical BigDecimal
// values.
type BigDecimalArray struct {
	array.ExtensionArrayBase
}

// EncodedValue returns the packed bytes for position i. The returned bytes
// are owned by the array and must not be modified.
func (a *BigDecimalArray) EncodedValue(i int) []byte {
	return a.Storage().(*array.BinaryView).Value(i)
}

// Value decodes the value at position i. Its behavior is undefined for a null
// row; callers must check IsNull first. It panics if the stored value is
// malformed; callers handling untrusted arrays should call array.ValidateFull
// first.
func (a *BigDecimalArray) Value(i int) BigDecimal {
	value, err := DecodeBigDecimal(a.EncodedValue(i))
	if err != nil {
		panic(err)
	}
	return value
}

func (a *BigDecimalArray) ValueStr(i int) string {
	if a.IsNull(i) {
		return array.NullValueStr
	}
	return a.Value(i).String()
}

func (a *BigDecimalArray) String() string {
	var out strings.Builder
	out.WriteByte('[')
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			out.WriteByte(' ')
		}
		if a.IsNull(i) {
			out.WriteString(array.NullValueStr)
		} else {
			fmt.Fprintf(&out, "%q", a.ValueStr(i))
		}
	}
	out.WriteByte(']')
	return out.String()
}

func (a *BigDecimalArray) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.ValueStr(i)
}

func (a *BigDecimalArray) MarshalJSON() ([]byte, error) {
	values := make([]interface{}, a.Len())
	for i := range values {
		values[i] = a.GetOneForMarshal(i)
	}
	return json.Marshal(values)
}

// Validate performs O(1) validation of the underlying BinaryView layout.
func (a *BigDecimalArray) Validate() error {
	storage, ok := a.Storage().(*array.BinaryView)
	if !ok {
		return fmt.Errorf("%w: BigDecimal storage is %T, expected *array.BinaryView", arrow.ErrInvalid, a.Storage())
	}
	return storage.Validate()
}

// ValidateFull validates the BinaryView references and every packed
// BigDecimal value. Informational extension metadata is not used to reject
// row values.
func (a *BigDecimalArray) ValidateFull() error {
	storage, ok := a.Storage().(*array.BinaryView)
	if !ok {
		return fmt.Errorf("%w: BigDecimal storage is %T, expected *array.BinaryView", arrow.ErrInvalid, a.Storage())
	}
	if err := storage.ValidateFull(); err != nil {
		return err
	}
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) {
			continue
		}
		_, err := DecodeBigDecimal(storage.Value(i))
		if err != nil {
			return fmt.Errorf("BigDecimal value at index %d: %w", i, err)
		}
	}
	return nil
}

// BigDecimalBuilder builds BigDecimal extension arrays using BinaryView
// storage.
type BigDecimalBuilder struct {
	*array.ExtensionBuilder
}

func NewBigDecimalBuilder(mem memory.Allocator, typ *BigDecimalType) *BigDecimalBuilder {
	if typ == nil {
		panic("extensions: BigDecimal type must not be nil")
	}
	return &BigDecimalBuilder{
		ExtensionBuilder: array.NewExtensionBuilder(mem, typ),
	}
}

// Append validates and appends a logical BigDecimal value.
func (b *BigDecimalBuilder) Append(value BigDecimal) error {
	encoded, err := EncodeBigDecimal(value)
	if err != nil {
		return err
	}
	b.StorageBuilder().(*array.BinaryViewBuilder).Append(encoded)
	return nil
}

// AppendEncoded validates and appends a packed BigDecimal value.
func (b *BigDecimalBuilder) AppendEncoded(encoded []byte) error {
	if _, err := DecodeBigDecimal(encoded); err != nil {
		return err
	}
	b.StorageBuilder().(*array.BinaryViewBuilder).Append(encoded)
	return nil
}

// AppendValues validates all values before appending them.
func (b *BigDecimalBuilder) AppendValues(values []BigDecimal, valid []bool) error {
	if len(valid) != 0 && len(valid) != len(values) {
		return fmt.Errorf("%w: len(values) != len(valid)", arrow.ErrInvalid)
	}

	encoded := make([][]byte, len(values))
	for i, value := range values {
		if len(valid) != 0 && !valid[i] {
			continue
		}
		var err error
		encoded[i], err = EncodeBigDecimal(value)
		if err != nil {
			return fmt.Errorf("BigDecimal value at index %d: %w", i, err)
		}
	}
	b.StorageBuilder().(*array.BinaryViewBuilder).AppendValues(encoded, valid)
	return nil
}

func (b *BigDecimalBuilder) AppendValueFromString(value string) error {
	if value == array.NullValueStr {
		b.AppendNull()
		return nil
	}
	parsed, err := ParseBigDecimal(value)
	if err != nil {
		return err
	}
	return b.Append(parsed)
}

func (b *BigDecimalBuilder) UnmarshalOne(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	switch value := token.(type) {
	case string:
		return b.AppendValueFromString(value)
	case nil:
		b.AppendNull()
		return nil
	default:
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(token),
			Type:   reflect.TypeOf(""),
			Offset: dec.InputOffset(),
			Struct: "BigDecimalBuilder",
		}
	}
}

func (b *BigDecimalBuilder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *BigDecimalBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("BigDecimal builder must unpack from JSON array, found %s", token)
	}
	return b.Unmarshal(dec)
}

func (b *BigDecimalBuilder) NewBigDecimalArray() *BigDecimalArray {
	return b.NewExtensionArray().(*BigDecimalArray)
}

func validateDecimalTarget(precision, scale, maxPrecision int32) error {
	if precision <= 0 || precision > maxPrecision {
		return fmt.Errorf("%w: decimal precision must be between 1 and %d, got %d",
			arrow.ErrInvalid, maxPrecision, precision)
	}
	if scale < -precision || scale > precision {
		return fmt.Errorf("%w: decimal scale must be between -precision and precision, got %d",
			arrow.ErrInvalid, scale)
	}
	return nil
}

func bigDecimalCoefficientAtScale(value BigDecimal, targetPrecision, targetScale int32) (*big.Int, error) {
	if !value.IsFinite() {
		return nil, fmt.Errorf("%w: cannot convert non-finite BigDecimal value %s to Arrow decimal",
			arrow.ErrInvalid, value)
	}

	coefficient := value.Magnitude()
	if value.IsNegative() {
		coefficient.Neg(coefficient)
	}

	delta := int64(targetScale) - int64(value.Scale())
	if delta != 0 {
		exponent := big.NewInt(delta)
		if delta < 0 {
			exponent.Neg(exponent)
		}
		multiplier := new(big.Int).Exp(big.NewInt(10), exponent, nil)
		if delta > 0 {
			coefficient.Mul(coefficient, multiplier)
		} else {
			quotient, remainder := new(big.Int), new(big.Int)
			quotient.QuoRem(coefficient, multiplier, remainder)
			if remainder.Sign() != 0 {
				return nil, fmt.Errorf("%w: converting BigDecimal scale %d to scale %d would lose data",
					arrow.ErrInvalid, value.Scale(), targetScale)
			}
			coefficient = quotient
		}
	}

	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(targetPrecision)), nil)
	if new(big.Int).Abs(coefficient).Cmp(limit) >= 0 {
		return nil, fmt.Errorf("%w: BigDecimal value does not fit decimal precision %d",
			arrow.ErrInvalid, targetPrecision)
	}
	return coefficient, nil
}

// ToDecimal128 converts a BigDecimal array to a fixed-precision Decimal128
// array. Conversion is exact: non-finite values, overflow, and rescaling that
// would discard nonzero digits return an error.
func (a *BigDecimalArray) ToDecimal128(mem memory.Allocator, typ *arrow.Decimal128Type) (*array.Decimal128, error) {
	if typ == nil {
		return nil, fmt.Errorf("%w: Decimal128 target type must not be nil", arrow.ErrInvalid)
	}
	if err := validateDecimalTarget(typ.Precision, typ.Scale, decimal128.MaxPrecision); err != nil {
		return nil, err
	}
	storage, ok := a.Storage().(*array.BinaryView)
	if !ok {
		return nil, fmt.Errorf("%w: BigDecimal storage is %T, expected *array.BinaryView", arrow.ErrInvalid, a.Storage())
	}
	if err := storage.ValidateFull(); err != nil {
		return nil, err
	}

	builder := array.NewDecimal128Builder(mem, typ)
	defer builder.Release()
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) {
			builder.AppendNull()
			continue
		}

		value, err := DecodeBigDecimal(storage.Value(i))
		if err != nil {
			return nil, fmt.Errorf("BigDecimal value at index %d: %w", i, err)
		}
		coefficient, err := bigDecimalCoefficientAtScale(value, typ.Precision, typ.Scale)
		if err != nil {
			return nil, fmt.Errorf("BigDecimal value at index %d: %w", i, err)
		}
		builder.Append(decimal128.FromBigInt(coefficient))
	}
	return builder.NewDecimal128Array(), nil
}

// ToDecimal256 converts a BigDecimal array to a fixed-precision Decimal256
// array. Conversion is exact: non-finite values, overflow, and rescaling that
// would discard nonzero digits return an error.
func (a *BigDecimalArray) ToDecimal256(mem memory.Allocator, typ *arrow.Decimal256Type) (*array.Decimal256, error) {
	if typ == nil {
		return nil, fmt.Errorf("%w: Decimal256 target type must not be nil", arrow.ErrInvalid)
	}
	if err := validateDecimalTarget(typ.Precision, typ.Scale, decimal256.MaxPrecision); err != nil {
		return nil, err
	}
	storage, ok := a.Storage().(*array.BinaryView)
	if !ok {
		return nil, fmt.Errorf("%w: BigDecimal storage is %T, expected *array.BinaryView", arrow.ErrInvalid, a.Storage())
	}
	if err := storage.ValidateFull(); err != nil {
		return nil, err
	}

	builder := array.NewDecimal256Builder(mem, typ)
	defer builder.Release()
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) {
			builder.AppendNull()
			continue
		}

		value, err := DecodeBigDecimal(storage.Value(i))
		if err != nil {
			return nil, fmt.Errorf("BigDecimal value at index %d: %w", i, err)
		}
		coefficient, err := bigDecimalCoefficientAtScale(value, typ.Precision, typ.Scale)
		if err != nil {
			return nil, fmt.Errorf("BigDecimal value at index %d: %w", i, err)
		}
		builder.Append(decimal256.FromBigInt(coefficient))
	}
	return builder.NewDecimal256Array(), nil
}

var (
	_ arrow.ExtensionType          = (*BigDecimalType)(nil)
	_ array.CustomExtensionBuilder = (*BigDecimalType)(nil)
	_ array.ExtensionArray         = (*BigDecimalArray)(nil)
	_ array.Validator              = (*BigDecimalArray)(nil)
	_ array.Builder                = (*BigDecimalBuilder)(nil)
)

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

package schema

import (
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
)

// Column encapsulates the information necessary to interpret primitive
// column data in the context of a particular schema. We have to examine
// the node structure of a column's path to the root in the schema tree
// to be able to reassemble the nested structure from the repetition and
// definition levels.
type Column struct {
	pnode *PrimitiveNode
	// the maximum definition level in this column
	// if this is > 0 then either this column or a parent column must be optional.
	maxDefLvl int16
	// the maximum repetition level in this column
	// if this is > 0, then either this column or a parent column must be repeated.
	// when the repetition level in the column data equals this value, it indicates
	// additional elements in the innermost list.
	maxRepLvl int16
	// cached path string to avoid repeated string building
	path string
	// effectiveVectorLength is the fixed number of leaf values contributed per
	// parent record when this column is part of a VECTOR-repeated subtree
	// (Option B), and -1 otherwise. See effectiveVectorLength.
	effectiveVectorLength int32
}

// effectiveVectorLength returns the product of vector_length over the leaf node
// and any ancestors that are VECTOR-repeated, which is the number of physical
// leaf values contributed per parent record. It returns -1 when the leaf is not
// part of a VECTOR-repeated subtree. The product is accumulated in int64 so a
// pathological nested-vector schema that would overflow int32 returns an error
// rather than silently wrapping.
func effectiveVectorLength(n Node) (int32, error) {
	eff := int64(-1)
	for cursor := n; cursor != nil; cursor = cursor.Parent() {
		if cursor.RepetitionType() != parquet.Repetitions.Vector {
			continue
		}
		vl := int64(nodeVectorLength(cursor))
		if eff < 0 {
			eff = vl
			continue
		}
		eff *= vl
		if eff > math.MaxInt32 {
			return 0, fmt.Errorf("parquet: nested VECTOR effective vector_length overflows int32 for column %q", n.Path())
		}
	}
	return int32(eff), nil
}

// NewColumn returns a new column object for the given node with the provided
// maximum definition and repetition levels. It panics if the schema is invalid
// (for example a nested-VECTOR effective vector_length that overflows int32);
// use NewColumnChecked to receive that as an error instead.
func NewColumn(n *PrimitiveNode, maxDefinitionLvl, maxRepetitionLvl int16) *Column {
	c, err := NewColumnChecked(n, maxDefinitionLvl, maxRepetitionLvl)
	if err != nil {
		panic(err)
	}
	return c
}

// NewColumnChecked is like NewColumn but returns an error instead of panicking
// when the schema is invalid, so it can be used on paths (such as
// NewSchemaChecked) that read untrusted files and must fail loudly rather than
// panic.
func NewColumnChecked(n *PrimitiveNode, maxDefinitionLvl, maxRepetitionLvl int16) (*Column, error) {
	eff, err := effectiveVectorLength(n)
	if err != nil {
		return nil, err
	}
	return &Column{
		pnode:                 n,
		maxDefLvl:             maxDefinitionLvl,
		maxRepLvl:             maxRepetitionLvl,
		path:                  n.Path(),
		effectiveVectorLength: eff,
	}, nil
}

// Name is the column's name
func (c *Column) Name() string { return c.pnode.Name() }

// ColumnPath returns the full path to this column from the root of the schema
func (c *Column) ColumnPath() parquet.ColumnPath { return c.pnode.columnPath() }

// Path is equivalent to ColumnPath().String() returning the dot-string version of the path
func (c *Column) Path() string { return c.path }

// TypeLength is -1 if not a FixedLenByteArray, otherwise it is the length of elements in the column
func (c *Column) TypeLength() int { return c.pnode.TypeLength() }

func (c *Column) MaxDefinitionLevel() int16 { return c.maxDefLvl }
func (c *Column) MaxRepetitionLevel() int16 { return c.maxRepLvl }

// EffectiveVectorLength returns the fixed number of leaf values per parent
// record contributed by this column when it belongs to a VECTOR-repeated
// subtree (the product of all VECTOR-repeated nodes on the path), or -1
// otherwise.
func (c *Column) EffectiveVectorLength() int32 { return c.effectiveVectorLength }

// InVectorColumn reports whether this column belongs to a VECTOR-repeated
// subtree (Option B), in which case leaf values are written without per-element
// repetition levels.
func (c *Column) InVectorColumn() bool { return c.effectiveVectorLength > 0 }

func (c *Column) PhysicalType() parquet.Type       { return c.pnode.PhysicalType() }
func (c *Column) ConvertedType() ConvertedType     { return c.pnode.convertedType }
func (c *Column) LogicalType() LogicalType         { return c.pnode.logicalType }
func (c *Column) ColumnOrder() parquet.ColumnOrder { return c.pnode.ColumnOrder }
func (c *Column) String() string {
	var bld strings.Builder
	bld.WriteString("column descriptor = {\n")
	fmt.Fprintf(&bld, "  name: %s,\n", c.Name())
	fmt.Fprintf(&bld, "  path: %s,\n", c.Path())
	fmt.Fprintf(&bld, "  physical_type: %s,\n", c.PhysicalType())
	fmt.Fprintf(&bld, "  converted_type: %s,\n", c.ConvertedType())
	fmt.Fprintf(&bld, "  logical_type: %s,\n", c.LogicalType())
	if c.InVectorColumn() {
		fmt.Fprintf(&bld, "  vector_length: %d,\n", c.EffectiveVectorLength())
	}
	fmt.Fprintf(&bld, "  max_definition_level: %d,\n", c.MaxDefinitionLevel())
	fmt.Fprintf(&bld, "  max_repetition_level: %d,\n", c.MaxRepetitionLevel())
	if c.PhysicalType() == parquet.Types.FixedLenByteArray {
		fmt.Fprintf(&bld, "  length: %d,\n", c.TypeLength())
	}
	if c.ConvertedType() == ConvertedTypes.Decimal {
		fmt.Fprintf(&bld, "  precision: %d,\n  scale: %d,\n", c.pnode.decimalMetaData.Precision, c.pnode.decimalMetaData.Scale)
	}
	bld.WriteString("}")
	return bld.String()
}

// Equals will return true if the rhs Column has the same Max Repetition and Definition levels
// along with having the same node definition.
func (c *Column) Equals(rhs *Column) bool {
	return c.pnode.Equals(rhs.pnode) &&
		c.MaxRepetitionLevel() == rhs.MaxRepetitionLevel() &&
		c.MaxDefinitionLevel() == rhs.MaxDefinitionLevel() &&
		c.EffectiveVectorLength() == rhs.EffectiveVectorLength()
}

// SchemaNode returns the underlying Node in the schema tree for this column.
func (c *Column) SchemaNode() Node {
	return c.pnode
}

// SortOrder returns the sort order of this column's statistics based on the
// Logical and Converted types.
func (c *Column) SortOrder() SortOrder {
	if c.LogicalType() != nil {
		return GetLogicalSortOrder(c.LogicalType(), format.Type(c.pnode.PhysicalType()))
	}
	return GetSortOrder(c.ConvertedType(), format.Type(c.pnode.PhysicalType()))
}

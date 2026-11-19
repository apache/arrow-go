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

package schemas

import (
	"errors"
	"fmt"
	"slices"

	"maps"

	"github.com/apache/arrow-go/v18/arrow"
)

var (
	ErrPathNotFound     = errors.New("path not found")
	ErrFieldTypeChanged = errors.New("type changed")
)

type schemaUnifier struct {
	old            *treeNode
	new            *treeNode
	typeConversion bool
}

type treeNode struct {
	root         *treeNode
	parent       *treeNode
	name         string
	path         []string
	field        arrow.Field
	children     []*treeNode
	childmap     map[string]*treeNode
	metadatas    arrow.Metadata
	index, depth int32
	err          error
}

// UnifySchemas unifies multiple schemas into a single schema. If promotePermissive is true, the unification process will promote integer types to larger integer types, integer types to floating-point types, STRING to LARGE_STRING, LIST to LARGE_LIST and LIST_VIEW to LARGE_LIST_VIEW. If promotePermissive is false, the unification process will not allow type conversion and will return an error if a type conflict is found.
func UnifySchemas(promotePermissive bool, schemas ...*arrow.Schema) (*arrow.Schema, error) {
	if len(schemas) < 2 {
		return nil, fmt.Errorf("not enough schemas to unify")
	}
	var u schemaUnifier
	u.typeConversion = promotePermissive
	u.old = newTreeFromSchema(schemas[0])
	for _, s := range schemas[1:] {
		u.new = newTreeFromSchema(s)
		u.unify()
	}
	if err := collectErrors(u.old); err != nil {
		return nil, err
	}
	return u.old.schema()
}

func newTreeRoot() *treeNode {
	f := new(treeNode)
	f.index = -1
	f.root = f
	f.name = "root"
	f.childmap = make(map[string]*treeNode)
	f.children = make([]*treeNode, 0)
	return f
}

func (f *treeNode) assignChild(child *treeNode) {
	f.children = append(f.children, child)
	f.childmap[child.name] = child
}

func (f *treeNode) newChild(childName string) *treeNode {
	var child treeNode = treeNode{
		root:   f.root,
		parent: f,
		name:   childName,
		index:  int32(len(f.children)),
		depth:  f.depth + 1,
	}
	child.path = child.namePath()
	child.childmap = make(map[string]*treeNode)
	return &child
}

func (f *treeNode) mapChildren() {
	for i, c := range f.children {
		f.childmap[c.name] = f.children[i]
	}
}

// getPath returns a field found at a defined path, otherwise returns ErrPathNotFound.
func (f *treeNode) getPath(path []string) (*treeNode, error) {
	if len(path) == 0 { // degenerate input
		return nil, fmt.Errorf("getPath needs at least one key")
	}
	if node, ok := f.childmap[path[0]]; !ok {
		return nil, ErrPathNotFound
	} else if len(path) == 1 { // we've reached the final key
		return node, nil
	} else { // 1+ more keys
		return node.getPath(path[1:])
	}
}

// namePath returns a slice of keys making up the path to the field
func (f *treeNode) namePath() []string {
	if len(f.path) == 0 {
		var path []string
		cur := f
		for i := f.depth - 1; i >= 0; i-- {
			path = append([]string{cur.name}, path...)
			cur = cur.parent
		}
		return path
	}
	return f.path
}

// dotPath returns the path to the field in json dot notation
func (f *treeNode) dotPath() string {
	var path string = "$"
	for i, p := range f.path {
		path = path + p
		if i+1 != len(f.path) {
			path = path + "."
		}
	}
	return path
}

// graft grafts a new field into the schema tree as a child of f
func (f *treeNode) graft(n *treeNode) {
	fIsNullable := f.field.Nullable
	graft := f.newChild(n.name)
	graft.field = n.field
	graft.children = append(graft.children, n.children...)
	graft.mapChildren()
	f.assignChild(graft)

	if !(f.root == f) && f.field.Type.ID() == arrow.STRUCT {
		gf := f.field.Type.(*arrow.StructType)
		var nf []arrow.Field
		nf = append(nf, gf.Fields()...)
		nf = append(nf, graft.field)
		f.field = arrow.Field{Name: f.name, Type: arrow.StructOf(nf...), Nullable: fIsNullable}
		if !(f.parent.name == "root") && (f.parent != nil) && f.parent.field.Type.ID() == arrow.LIST {
			f.parent.field = arrow.Field{Name: f.parent.name, Type: arrow.ListOfField(f.field)}
		}
	}
}

func collectErrors(f *treeNode) error {
	var err error
	if f.err != nil {
		err = errors.Join(err, f.err)
	}
	for _, field := range f.children {
		err = errors.Join(err, collectErrors(field))
	}
	return err
}

func newTreeFromSchema(a *arrow.Schema) *treeNode {
	f := newTreeRoot()
	f.metadatas = a.Metadata()
	treeFromSchema(f, a)
	return f
}

func treeFromSchema(f *treeNode, a *arrow.Schema) {
	for _, field := range a.Fields() {
		child := f.newChild(field.Name)
		child.field = field
		child.metadatas = field.Metadata

		switch field.Type.ID() {
		case arrow.STRUCT:
			structType := field.Type.(*arrow.StructType)
			for _, subField := range structType.Fields() {
				subChild := child.newChild(subField.Name)
				subChild.field = subField
				child.assignChild(subChild)
				treeFromSchema(subChild, arrow.NewSchema([]arrow.Field{subField}, nil))
			}
		case arrow.LIST:
			listType := field.Type.(*arrow.ListType)
			elemField := arrow.Field{Name: "element", Type: listType.Elem()}
			elemChild := child.newChild("element")
			elemChild.field = elemField
			child.assignChild(elemChild)
			treeFromSchema(elemChild, arrow.NewSchema([]arrow.Field{elemField}, nil))
		case arrow.MAP:
			mapType := field.Type.(*arrow.MapType)
			keyField := arrow.Field{Name: "key", Type: mapType.KeyType()}
			valueField := arrow.Field{Name: "value", Type: mapType.ItemType()}
			keyChild := child.newChild("key")
			valueChild := child.newChild("value")
			keyChild.field = keyField
			valueChild.field = valueField
			child.assignChild(keyChild)
			child.assignChild(valueChild)
			treeFromSchema(keyChild, arrow.NewSchema([]arrow.Field{keyField}, nil))
			treeFromSchema(valueChild, arrow.NewSchema([]arrow.Field{valueField}, nil))
		case arrow.RUN_END_ENCODED:
			runEndEncodedType := field.Type.(*arrow.RunEndEncodedType)
			runEndField := arrow.Field{Name: "run_ends", Type: runEndEncodedType.RunEnds()}
			valuesField := arrow.Field{Name: "values", Type: runEndEncodedType.Encoded()}
			runEndChild := child.newChild("run_ends")
			valuesChild := child.newChild("values")
			runEndChild.field = runEndField
			valuesChild.field = valuesField
			child.assignChild(runEndChild)
			child.assignChild(valuesChild)
			treeFromSchema(runEndChild, arrow.NewSchema([]arrow.Field{runEndField}, nil))
			treeFromSchema(valuesChild, arrow.NewSchema([]arrow.Field{valuesField}, nil))
		case arrow.DICTIONARY:
			dictType := field.Type.(*arrow.DictionaryType)
			indexField := arrow.Field{Name: "index", Type: dictType.IndexType}
			valueField := arrow.Field{Name: "dictionary", Type: dictType.ValueType}
			indexChild := child.newChild("index")
			valueChild := child.newChild("dictionary")
			indexChild.field = indexField
			valueChild.field = valueField
			child.assignChild(indexChild)
			child.assignChild(valueChild)
			treeFromSchema(indexChild, arrow.NewSchema([]arrow.Field{indexField}, nil))
			treeFromSchema(valueChild, arrow.NewSchema([]arrow.Field{valueField}, nil))
		default:
			// Scalar types do not need further processing
		}

		f.assignChild(child)
	}
}

func (f *treeNode) schema() (*arrow.Schema, error) {
	var s *arrow.Schema
	defer func(s *arrow.Schema) (*arrow.Schema, error) {
		if pErr := recover(); pErr != nil {
			return nil, fmt.Errorf("schema problem: %v", pErr)
		}
		return s, nil
	}(s)
	var fields []arrow.Field
	for _, c := range f.children {
		fields = append(fields, c.field)
	}
	s = arrow.NewSchema(fields, &f.metadatas)
	return s, nil
}

func (o *treeNode) upgradeType(n *treeNode, t arrow.DataType) {
	// Update the current node's field and type
	o.field = arrow.Field{Name: o.name, Type: t, Nullable: mergeBool(o.field.Nullable, n.field.Nullable)}

	// Update the parent node if it exists and is not the root
	if o.parent != o.root {
		switch o.parent.field.Type.ID() {
		case arrow.LIST:
			// Update the parent LIST type to reflect the upgraded child type
			o.parent.field = arrow.Field{Name: o.parent.name, Type: arrow.ListOfField(o.field)}
		case arrow.LARGE_LIST:
			// Update the parent LARGE_LIST type to reflect the upgraded child type
			o.parent.field = arrow.Field{Name: o.parent.name, Type: arrow.LargeListOfField(o.field)}
		case arrow.LIST_VIEW:
			// Update the parent LIST_VIEW type to reflect the upgraded child type
			o.parent.field = arrow.Field{Name: o.parent.name, Type: arrow.ListViewOfField(o.field)}
		case arrow.LARGE_LIST_VIEW:
			// Update the parent LARGE_LIST_VIEW type to reflect the upgraded child type
			o.parent.field = arrow.Field{Name: o.parent.name, Type: arrow.LargeListViewOfField(o.field)}
		case arrow.STRUCT:
			// Update the parent STRUCT type to include the upgraded child field
			var fields []arrow.Field
			for _, c := range o.parent.children {
				fields = append(fields, c.field)
			}
			o.parent.field = arrow.Field{
				Name:     o.parent.name,
				Type:     arrow.StructOf(fields...),
				Nullable: mergeBool(o.parent.field.Nullable, n.parent.field.Nullable),
			}

		case arrow.MAP:
			// Update the parent MAP type to reflect the upgraded key or value type
			var keyType, valueType arrow.DataType
			for _, c := range o.parent.children {
				if c.name == "key" {
					keyType = c.field.Type
				} else if c.name == "value" {
					valueType = c.field.Type
				}
			}
			o.parent.field = arrow.Field{
				Name:     o.parent.name,
				Type:     arrow.MapOf(keyType, valueType),
				Nullable: mergeBool(o.parent.field.Nullable, n.parent.field.Nullable),
			}

		case arrow.DICTIONARY:
			// Update the parent DICTIONARY type to reflect the upgraded index or value type
			var indexType, valueType arrow.DataType
			for _, c := range o.parent.children {
				if c.name == "index" {
					indexType = c.field.Type
				} else if c.name == "dictionary" {
					valueType = c.field.Type
				}
			}
			o.parent.field = arrow.Field{
				Name:     o.parent.name,
				Type:     &arrow.DictionaryType{IndexType: indexType, ValueType: valueType},
				Nullable: mergeBool(o.parent.field.Nullable, n.parent.field.Nullable),
			}

		case arrow.RUN_END_ENCODED:
			// Update the parent RUN_END_ENCODED type to reflect the upgraded run_ends or values type
			var runEndsType, valuesType arrow.DataType
			for _, c := range o.parent.children {
				if c.name == "run_ends" {
					runEndsType = c.field.Type
				} else if c.name == "values" {
					valuesType = c.field.Type
				}
			}
			o.parent.field = arrow.Field{
				Name:     o.parent.name,
				Type:     arrow.RunEndEncodedOf(runEndsType, valuesType),
				Nullable: mergeBool(o.parent.field.Nullable, n.parent.field.Nullable),
			}
		}
	}
}

func (u *schemaUnifier) unify() {
	if u.new.metadatas.Len() > 0 {
		u.old.mergeMetadata(u.new.metadatas)
	}
	for _, field := range u.new.children {
		u.merge(field, nil)
	}
}

// merge merges a new or changed field into the unified schema.
func (u *schemaUnifier) merge(n *treeNode, mergeAt []string) {
	var nPath, nParentPath []string
	if len(mergeAt) > 0 {
		nPath = slices.Concat(mergeAt, n.path)
		nParentPath = slices.Concat(mergeAt, n.parent.path)
	} else {
		nPath = n.path
		nParentPath = n.parent.path
	}
	// if field in new schema is not found in old schema, graft it in.
	if kin, err := u.old.getPath(nPath); err == ErrPathNotFound {
		// root graft
		if n.root == n.parent {
			u.old.root.graft(n)
		} else {
			// branch graft
			b, _ := u.old.getPath(nParentPath)
			b.graft(n)
		}
	} else {
		// if field in new schema is found in old schema, check for type changes.
		if n.field.HasMetadata() {
			kin.mergeMetadata(n.field.Metadata)
		}
		// no type conversion
		if !u.typeConversion && (!kin.field.Equal(n.field) && kin.field.Type.ID() != n.field.Type.ID()) {
			kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
		}
		// promote nullability
		if u.typeConversion && (!kin.field.Equal(n.field) && kin.field.Type.ID() == n.field.Type.ID() && kin.field.Nullable != n.field.Nullable) {
			kin.upgradeType(n, n.field.Type)
		}
		// type convertion
		if u.typeConversion && (!kin.field.Equal(n.field) && kin.field.Type.ID() != n.field.Type.ID()) {
			// handle integers and floats
			switch kt := kin.field.Type.(type) {
			case arrow.FixedWidthDataType:
				switch nt := n.field.Type.(type) {
				case arrow.FixedWidthDataType:
					// unsigned integers can be upgraded to types with double the bit width
					if arrow.IsUnsignedInteger(kt.ID()) {
						if kt.BitWidth()*2 == nt.BitWidth() {
							kin.upgradeType(n, n.field.Type)
						} else {
							kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
						}
					}
					//  integers can be upgraded to integer types of a higher bit width
					if arrow.IsSignedInteger(kt.ID()) && arrow.IsSignedInteger(nt.ID()) {
						if kt.BitWidth() < nt.BitWidth() {
							kin.upgradeType(n, n.field.Type)
						} else {
							kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
						}
					}
					// ints can be upgraded to floating point types of a higher bit width
					if arrow.IsSignedInteger(kt.ID()) && arrow.IsFloating(nt.ID()) {
						if kt.BitWidth() < nt.BitWidth() {
							kin.upgradeType(n, n.field.Type)
						} else {
							kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
						}
					}
				default:
					kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
				}
			default:
				switch kin.field.Type.ID() {
				// NULL upgrades to any type - no type can be downgraded to NULL
				case arrow.NULL:
					kin.upgradeType(n, n.field.Type)
				// STRING can be upgraded to LARGE_STRING
				case arrow.STRING:
					switch n.field.Type.ID() {
					case arrow.LARGE_STRING:
						kin.upgradeType(n, n.field.Type)
					default:
						kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
						return
					}
				// BINARY can be upgraded to LARGE_BINARY
				case arrow.BINARY:
					switch n.field.Type.ID() {
					case arrow.LARGE_BINARY:
						kin.upgradeType(n, n.field.Type)
					default:
						kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
						return
					}
				// LIST can be upgraded to LARGE_LIST
				case arrow.LIST:
					switch n.field.Type.ID() {
					case arrow.LARGE_LIST:
						kin.upgradeType(n, n.field.Type)
					default:
						kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
						return
					}
				// LIST_VIEW can be upgraded to LARGE_LIST_VIEW
				case arrow.LIST_VIEW:
					switch n.field.Type.ID() {
					case arrow.LARGE_LIST_VIEW:
						kin.upgradeType(n, n.field.Type)
					default:
						kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
						return
					}
				//
				default:
					kin.err = errors.Join(kin.err, fmt.Errorf("%s %v to %v", n.dotPath(), ErrFieldTypeChanged.Error(), n.field.Type.Name()))
					return
				}
			}

		}

	}
	for _, v := range n.childmap {
		u.merge(v, mergeAt)
	}
}

func (f *treeNode) mergeMetadata(m1 arrow.Metadata) {
	merged := make(map[string]string)

	maps.Copy(merged, m1.ToMap())
	maps.Copy(merged, f.metadatas.ToMap())
	f.field.Metadata = arrow.MetadataFrom(merged)
}

func mergeBool(a, b bool) bool {
	return a || b
}

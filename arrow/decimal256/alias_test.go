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

package decimal256

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed alias.go
var aliasSource []byte

// TestInlineStubCoversAllNumMethods fails when the inlining stub at the bottom of
// alias.go stops matching the method set of Num, which silently rebreaks
// https://github.com/golang/go/issues/82474 for importers of this package.
func TestInlineStubCoversAllNumMethods(t *testing.T) {
	typ := reflect.TypeOf(Num{})
	want := make([]string, 0, typ.NumMethod())
	for i := range typ.NumMethod() {
		want = append(want, typ.Method(i).Name)
	}
	sort.Strings(want)

	got := inlinedMethods(t)
	sort.Strings(got)

	assert.Equal(t, want, got, "alias.go inlining stub must call every method of Num")
}

func inlinedMethods(t *testing.T) []string {
	t.Helper()

	f, err := parser.ParseFile(token.NewFileSet(), "alias.go", aliasSource, 0)
	require.NoError(t, err)

	var stub *ast.FuncLit
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, v := range vs.Values {
				lit, ok := v.(*ast.FuncLit)
				if !ok {
					continue
				}
				require.Nil(t, stub, "alias.go has more than one inlining stub")
				stub = lit
			}
		}
	}
	require.NotNil(t, stub, "alias.go inlining stub not found")
	require.Len(t, stub.Type.Params.List, 1)
	require.Len(t, stub.Type.Params.List[0].Names, 1)
	param := stub.Type.Params.List[0].Names[0].Name

	var methods []string
	ast.Inspect(stub.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == param {
			methods = append(methods, sel.Sel.Name)
		}
		return true
	})
	return methods
}

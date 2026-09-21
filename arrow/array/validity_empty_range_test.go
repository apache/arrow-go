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

package array_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrayEqualEmptyValidityRangePastBuffer(t *testing.T) {
	left := makeBooleanValidityEqualityArray(0, 0, nil, 0)
	defer left.Release()
	validity := memory.NewBufferBytes([]byte{0xff})
	defer validity.Release()
	right := makeBooleanValidityEqualityArray(0, 64, validity, 0)
	defer right.Release()

	assert.True(t, array.Equal(left, right))
	assert.True(t, array.Equal(right, left))
	assert.True(t, array.ApproxEqual(left, right))
	assert.True(t, array.ApproxEqual(right, left))
}

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

package decimal

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Array buffers, IPC endian swapping and scalars reinterpret Decimal256 values
// as four little endian uint64 limbs.
func TestDecimal256Layout(t *testing.T) {
	var n Decimal256
	assert.EqualValues(t, 32, unsafe.Sizeof(n))
	assert.EqualValues(t, 0, unsafe.Offsetof(n.arr))
	assert.Equal(t, [4]uint64{4, 3, 2, 1}, NewDecimal256(1, 2, 3, 4).arr)
}

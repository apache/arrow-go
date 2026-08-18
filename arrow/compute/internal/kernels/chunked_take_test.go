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

//go:build go1.18

package kernels

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestTakeChunkedBinaryRejectsInt32OffsetOverflow(t *testing.T) {
	require.NoError(t, checkBinaryTakeOffset[int32](0, math.MaxInt32))
	require.ErrorIs(t,
		checkBinaryTakeOffset[int32](0, int64(math.MaxInt32)+1),
		arrow.ErrInvalid)
	require.NoError(t, checkBinaryTakeOffset[int64](0, int64(math.MaxInt32)+1))
}

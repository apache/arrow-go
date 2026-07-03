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

package compress

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestZstdStreamingReaderPooled verifies NewReader reuses a pooled decoder across
// pages (Reset) instead of allocating a fresh one each call.
func TestZstdStreamingReaderPooled(t *testing.T) {
	c := zstdCodec{}
	comp := c.Encode(nil, bytes.Repeat([]byte("pool me "), 200))
	// io.MultiReader is not a byter, so Reset uses the streaming path (not DecodeAll).
	src := func() io.Reader { return io.MultiReader(bytes.NewReader(comp)) }

	rc1 := c.NewReader(src())
	dec1 := rc1.(*zstdcloser).Decoder
	out1, err := io.ReadAll(rc1)
	require.NoError(t, err)
	require.NoError(t, rc1.Close())

	rc2 := c.NewReader(src())
	dec2 := rc2.(*zstdcloser).Decoder
	out2, err := io.ReadAll(rc2)
	require.NoError(t, err)
	require.NoError(t, rc2.Close())

	require.Equal(t, out1, out2)
	require.Same(t, dec1, dec2, "streaming decoder should be reused from the pool")
}

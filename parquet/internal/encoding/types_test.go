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

package encoding

import (
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestBufferWriterOffset(t *testing.T) {
	writer := NewBufferWriter(0, memory.DefaultAllocator)
	defer writer.Release()

	writer.SetOffset(1024)
	require.Zero(t, writer.Len())

	n, err := writer.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), writer.Bytes())
	require.Equal(t, 5, writer.Len())
	require.Equal(t, int64(5), writer.Tell())
}

func TestBufferWriterUnsafeWriteCopyWithOffset(t *testing.T) {
	writer := NewBufferWriter(0, memory.DefaultAllocator)
	defer writer.Release()

	writer.SetOffset(4)
	writer.Reserve(4)

	n, err := writer.UnsafeWriteCopy(2, []byte("ab"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("abab"), writer.Bytes())
	require.Equal(t, 4, writer.Len())
}

func TestBufferWriterSeekWithOffset(t *testing.T) {
	writer := NewBufferWriter(0, memory.DefaultAllocator)
	defer writer.Release()

	writer.SetOffset(4)
	_, err := writer.Write([]byte("abc"))
	require.NoError(t, err)

	pos, err := writer.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	_, err = writer.Write([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte("xbc"), writer.Bytes())

	pos, err = writer.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(2), pos)

	_, err = writer.Write([]byte("y"))
	require.NoError(t, err)
	require.Equal(t, []byte("xby"), writer.Bytes())
}

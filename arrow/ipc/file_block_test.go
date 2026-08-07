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

package ipc

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
)

func testFramedMessage(t *testing.T, bodyLen int, legacy bool) ([]byte, int32, int64) {
	t.Helper()
	meta := writeMessageFB(flatbuffers.NewBuilder(0), memory.DefaultAllocator,
		flatbuf.MessageHeaderNONE, 0, int64(bodyLen), arrow.Metadata{})
	defer meta.Release()

	padding := (8 - meta.Len()%8) % 8
	prefix := 8
	if legacy {
		prefix = 4
	}
	framed := make([]byte, prefix+meta.Len()+padding)
	if legacy {
		binary.LittleEndian.PutUint32(framed, uint32(meta.Len()+padding))
	} else {
		binary.LittleEndian.PutUint32(framed, kIPCContToken)
		binary.LittleEndian.PutUint32(framed[4:], uint32(meta.Len()+padding))
	}
	copy(framed[prefix:], meta.Bytes())
	return framed, int32(len(framed)), int64(bodyLen)
}

func TestFileBlockNewMessageValidatesFraming(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, mapped := range []bool{false, true} {
			name := "continuation"
			if legacy {
				name = "legacy"
			}
			if mapped {
				name += " mapped"
			} else {
				name += " reader"
			}

			t.Run(name, func(t *testing.T) {
				framed, metaLen, bodyLen := testFramedMessage(t, 4, legacy)
				body := []byte{1, 2, 3, 4}

				newBlock := func(meta int32, blockBody int64) dataBlock {
					data := append(append([]byte{}, framed...), body...)
					if meta > metaLen {
						data = append(data[:int(metaLen)], make([]byte, int(meta-metaLen))...)
						data = append(data, body...)
					}
					if blockBody > bodyLen {
						data = append(data, make([]byte, int(blockBody-bodyLen))...)
					}
					if mapped {
						return mappedFileBlock{meta: meta, body: blockBody, data: data}
					}
					return fileBlock{meta: meta, body: blockBody, r: bytes.NewReader(data), mem: memory.DefaultAllocator}
				}

				msg, err := newBlock(metaLen, bodyLen).NewMessage()
				require.NoError(t, err)
				require.EqualValues(t, bodyLen, msg.BodyLen())
				msg.Release()

				_, err = newBlock(metaLen+8, bodyLen).NewMessage()
				require.ErrorContains(t, err, "metadata length prefix")
				_, err = newBlock(metaLen-4, bodyLen).NewMessage()
				require.ErrorContains(t, err, "metadata length prefix")
				_, err = newBlock(metaLen, bodyLen+1).NewMessage()
				require.ErrorContains(t, err, "body length")
				_, err = newBlock(metaLen, bodyLen-1).NewMessage()
				require.ErrorContains(t, err, "body length")
			})
		}
	}
}

func TestValidateFileBlockRejectsUnalignedBody(t *testing.T) {
	err := validateFileBlock(8, 8, 4, 24, 0, 0)
	require.ErrorContains(t, err, "not a multiple of 8")
}

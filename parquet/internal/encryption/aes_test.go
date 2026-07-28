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

package encryption

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingRandomReader struct {
	err error
}

func (r failingRandomReader) Read([]byte) (int, error) { return 0, r.err }

func TestAESEncryptRejectsNonceGenerationFailure(t *testing.T) {
	originalReadRandom := readRandom
	t.Cleanup(func() { readRandom = originalReadRandom })
	failingReader := failingRandomReader{err: errors.New("entropy unavailable")}
	readRandom = failingReader.Read

	for _, tc := range []struct {
		name string
		alg  parquet.Cipher
	}{
		{name: "GCM", alg: parquet.AesGcm},
		{name: "CTR", alg: parquet.AesCtr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encryptor := NewAesEncryptor(tc.alg, false)
			require.PanicsWithError(t,
				"parquet: failed to generate encryption nonce: entropy unavailable",
				func() { encryptor.Encrypt(&bytes.Buffer{}, []byte("data"), make([]byte, 16), nil) })
		})
	}
}

func TestAESDecryptRejectsMalformedCiphertext(t *testing.T) {
	decryptor := newAesDecryptor(parquet.AesGcm, false)
	key := make([]byte, 16)

	tests := map[string][]byte{
		"missing length": nil,
		"short length":   {0x01, 0x02, 0x03},
		"short payload":  framedCiphertext(NonceLength, NonceLength),
		"truncated":      framedCiphertext(NonceLength+GcmTagLength, 1),
		"trailing data":  framedCiphertext(NonceLength+GcmTagLength, NonceLength+GcmTagLength+1),
		"invalid tag":    framedCiphertext(NonceLength+GcmTagLength, NonceLength+GcmTagLength),
	}

	for name, ciphertext := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := decryptor.Decrypt(ciphertext, key, nil)
			assert.Error(t, err)
		})
	}
}

func TestAESDecryptFromValidatesLengthBeforeAllocation(t *testing.T) {
	decryptor := newAesDecryptor(parquet.AesGcm, false)
	key := make([]byte, 16)

	t.Run("exceeds limit", func(t *testing.T) {
		var input [4]byte
		binary.LittleEndian.PutUint32(input[:], 1<<30)
		_, err := decryptor.DecryptFrom(bytes.NewReader(input[:]), key, nil, 1024)
		assert.ErrorContains(t, err, "exceeds limit")
	})

	t.Run("short payload", func(t *testing.T) {
		input := framedCiphertext(NonceLength, NonceLength)
		_, err := decryptor.DecryptFrom(bytes.NewReader(input), key, nil, 1024)
		assert.ErrorContains(t, err, "smaller than minimum")
	})

	t.Run("truncated payload", func(t *testing.T) {
		input := framedCiphertext(NonceLength+GcmTagLength, 1)
		_, err := decryptor.DecryptFrom(bytes.NewReader(input), key, nil, 1024)
		assert.ErrorContains(t, err, "reading encrypted payload")
	})
}

func framedCiphertext(declared, actual int) []byte {
	data := make([]byte, bufferSizeLength+actual)
	binary.LittleEndian.PutUint32(data, uint32(declared))
	return data
}

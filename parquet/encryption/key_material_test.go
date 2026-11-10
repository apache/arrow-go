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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mapKMS resolves master key IDs to raw keys, standing in for a real KMS.
type mapKMS struct {
	keys map[string][]byte
}

func (m mapKMS) WrapKey(key []byte, masterKeyID string) (string, error) {
	return "", nil // unused by these tests
}

func (m mapKMS) UnwrapKey(wrappedKey string, masterKeyID string) ([]byte, error) {
	key, ok := m.keys[masterKeyID]
	if !ok {
		return nil, assert.AnError
	}
	return key, nil
}

// encryptKeyLocally mirrors parquet-cpp's EncryptKeyLocally for test
// fixtures: AES-GCM, nonce || ciphertext || tag, base64 encoded.
func encryptKeyLocally(t *testing.T, key, master, aad []byte) string {
	t.Helper()
	block, err := aes.NewCipher(master)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)
	require.NoError(t, err)
	sealed := gcm.Seal(nonce, nonce, key, aad)
	return base64.StdEncoding.EncodeToString(sealed)
}

func footerMetadata(t *testing.T, double bool) []byte {
	t.Helper()
	material := KeyMaterial{
		KeyMaterialType: "PKMT1",
		InternalStorage: true,
		IsFooterKey:     true,
		KmsInstanceID:   "kms-instance",
		KmsInstanceURL:  "https://kms.example",
		MasterKeyID:     "mk1",
		DoubleWrapping:  double,
	}
	if double {
		kek := bytes.Repeat([]byte{0x2a}, 16)
		material.KeyEncryptionKeyID = base64.StdEncoding.EncodeToString([]byte("kek-id"))
		material.WrappedKEK = "wrapped-kek"
		// a real wrapped DEK so the unwrapper has something valid to decrypt
		material.WrappedDEK = encryptKeyLocally(t, bytes.Repeat([]byte{0x11}, 16), kek, []byte("kek-id"))
	} else {
		material.WrappedDEK = "wrapped-dek"
	}
	encoded, err := json.Marshal(material)
	require.NoError(t, err)
	return encoded
}

func TestParseKeyMetadata(t *testing.T) {
	material, err := ParseKeyMetadata(footerMetadata(t, false))
	require.NoError(t, err)
	assert.Equal(t, "PKMT1", material.KeyMaterialType)
	assert.True(t, material.InternalStorage)
	assert.True(t, material.IsFooterKey)
	assert.Equal(t, "mk1", material.MasterKeyID)
	assert.False(t, material.DoubleWrapping)
}

func TestParseKeyMetadataErrors(t *testing.T) {
	_, err := ParseKeyMetadata([]byte(`{"keyMaterialType":"KM2","internalStorage":true}`))
	assert.ErrorContains(t, err, "unsupported key material type")

	_, err = ParseKeyMetadata([]byte(`{"keyMaterialType":"PKMT1","internalStorage":false,"keyReference":"ref"}`))
	assert.ErrorContains(t, err, "external key material storage")

	_, err = ParseKeyMetadata([]byte(`{"keyMaterialType":"PKMT1","internalStorage":true}`))
	assert.ErrorContains(t, err, "missing masterKeyID or wrappedDEK")

	_, err = ParseKeyMetadata([]byte(`{"keyMaterialType":"PKMT1","internalStorage":true,"masterKeyID":"m","wrappedDEK":"d","doubleWrapping":true}`))
	assert.ErrorContains(t, err, "missing keyEncryptionKeyID or wrappedKEK")

	_, err = ParseKeyMetadata([]byte(`{`))
	assert.ErrorContains(t, err, "parse key metadata")
}

func TestKmsKeyRetrieverSingleWrapping(t *testing.T) {
	dek := bytes.Repeat([]byte{0x42}, 16)
	kms := mapKMS{keys: map[string][]byte{"mk1": dek}}
	retriever := NewKmsKeyRetriever(kms)

	key, err := retriever.UnwrapKeyMetadata(footerMetadata(t, false))
	require.NoError(t, err)
	assert.Equal(t, dek, key)
	assert.Equal(t, string(dek), retriever.GetKey(footerMetadata(t, false)))
}

func TestKmsKeyRetrieverDoubleWrapping(t *testing.T) {
	kek := bytes.Repeat([]byte{0x2a}, 16)
	kms := mapKMS{keys: map[string][]byte{"mk1": kek}}
	retriever := NewKmsKeyRetriever(kms)

	key, err := retriever.UnwrapKeyMetadata(footerMetadata(t, true))
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{0x11}, 16), key)
}

func TestKmsKeyRetrieverGetKeyHidesErrors(t *testing.T) {
	retriever := NewKmsKeyRetriever(mapKMS{keys: map[string][]byte{}})
	assert.Empty(t, retriever.GetKey(footerMetadata(t, false)))
}

func TestDecryptKeyLocallyRejectsTampering(t *testing.T) {
	kek := bytes.Repeat([]byte{0x2a}, 16)
	wrapped, err := base64.StdEncoding.DecodeString(
		encryptKeyLocally(t, bytes.Repeat([]byte{0x11}, 16), kek, []byte("kek-id")))
	require.NoError(t, err)
	wrapped[0] ^= 0xff
	_, err = decryptKeyLocally(wrapped, kek, []byte("kek-id"))
	assert.Error(t, err)
}

// countingKMS counts UnwrapKey calls to observe the KEK cache.
type countingKMS struct {
	KmsClient
	unwraps atomic.Int32
}

func (c *countingKMS) UnwrapKey(wrappedKey string, masterKeyID string) ([]byte, error) {
	c.unwraps.Add(1)
	return c.KmsClient.UnwrapKey(wrappedKey, masterKeyID)
}

// doubleWrapMetadata returns double wrapping key metadata carrying kekID, as
// two files sharing one KEK would.
func doubleWrapMetadata(t *testing.T, kekID string) []byte {
	t.Helper()
	material := KeyMaterial{
		KeyMaterialType:    "PKMT1",
		InternalStorage:    true,
		IsFooterKey:        true,
		KmsInstanceID:      "kms-instance",
		KmsInstanceURL:     "https://kms.example",
		MasterKeyID:        "mk1",
		DoubleWrapping:     true,
		KeyEncryptionKeyID: base64.StdEncoding.EncodeToString([]byte(kekID)),
		WrappedKEK:         "wrapped-kek",
		WrappedDEK: encryptKeyLocally(t, bytes.Repeat([]byte{0x11}, 16),
			bytes.Repeat([]byte{0x2a}, 16), []byte(kekID)),
	}
	encoded, err := json.Marshal(material)
	require.NoError(t, err)
	return encoded
}

func newCountingRetriever() (*KmsKeyRetriever, *countingKMS) {
	kms := &countingKMS{KmsClient: mapKMS{keys: map[string][]byte{
		"mk1": bytes.Repeat([]byte{0x2a}, 16),
	}}}
	return NewKmsKeyRetriever(kms), kms
}

func TestKmsKeyRetrieverCachesKEKs(t *testing.T) {
	retriever, kms := newCountingRetriever()

	// two files sharing a KEK: same KEK ID, each with its own wrapped DEK
	for i := 0; i < 2; i++ {
		key, err := retriever.UnwrapKeyMetadata(doubleWrapMetadata(t, "kek-id"))
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{0x11}, 16), key)
	}
	assert.Equal(t, int32(1), kms.unwraps.Load(), "second file must hit the KEK cache")
}

func TestKmsKeyRetrieverKekCacheExpires(t *testing.T) {
	retriever, kms := newCountingRetriever()

	now := time.Now()
	retriever.keks.now = func() time.Time { return now }

	_, err := retriever.UnwrapKeyMetadata(doubleWrapMetadata(t, "kek-id"))
	require.NoError(t, err)

	// a rotated KEK written under the same ID becomes visible after the TTL
	now = now.Add(kekCacheTTL + time.Second)
	_, err = retriever.UnwrapKeyMetadata(doubleWrapMetadata(t, "kek-id"))
	require.NoError(t, err)
	assert.Equal(t, int32(2), kms.unwraps.Load(), "expired entry must miss the cache")
}

func TestKmsKeyRetrieverKekCacheSeparatesKeys(t *testing.T) {
	retriever, kms := newCountingRetriever()

	for _, kekID := range []string{"kek-id-1", "kek-id-2"} {
		_, err := retriever.UnwrapKeyMetadata(doubleWrapMetadata(t, kekID))
		require.NoError(t, err)
	}
	assert.Equal(t, int32(2), kms.unwraps.Load(), "distinct KEK IDs must not share a cache entry")
}

func TestKmsKeyRetrieverKekCacheCollapsesConcurrentMisses(t *testing.T) {
	retriever, kms := newCountingRetriever()

	// the footer and column reads of one file resolve the same KEK
	// concurrently: a single KMS call must serve all of them
	metadata := doubleWrapMetadata(t, "kek-id")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := retriever.UnwrapKeyMetadata(metadata)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Repeat([]byte{0x11}, 16), key)
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), kms.unwraps.Load())
}

func TestKekCacheEviction(t *testing.T) {
	cache := newKekCache(time.Minute, 2)
	now := time.Now()
	cache.now = func() time.Time { return now }

	cache.put(kekCacheKey{"mk", "k1"}, []byte{1})
	now = now.Add(2 * time.Minute) // k1 is now expired

	cache.put(kekCacheKey{"mk", "k2"}, []byte{2})
	assert.Len(t, cache.entries, 1, "inserting must drop expired entries first")

	cache.put(kekCacheKey{"mk", "k3"}, []byte{3})
	cache.put(kekCacheKey{"mk", "k4"}, []byte{4}) // full of live entries: evicts LRU k2

	assert.Nil(t, cache.get(kekCacheKey{"mk", "k2"}), "LRU entry must be evicted")
	assert.Equal(t, []byte{3}, cache.get(kekCacheKey{"mk", "k3"}))
	assert.Equal(t, []byte{4}, cache.get(kekCacheKey{"mk", "k4"}))
	assert.Len(t, cache.entries, 2)
}

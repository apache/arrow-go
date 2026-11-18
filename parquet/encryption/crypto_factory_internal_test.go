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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package encryption

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// evictKMS is the smallest working KmsClient: symmetric base64 wrapping.
type evictKMS struct{}

func (evictKMS) WrapKey(key []byte, _ string) (string, error) {
	return base64.StdEncoding.EncodeToString(key), nil
}

func (evictKMS) UnwrapKey(wrappedKey string, _ string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(wrappedKey)
}

// Expired cache entries must leave the map when a new entry joins it
// (parquet-cpp's CheckCacheForExpiredTokens): with rotating access tokens
// the cache must not keep every stale plaintext KEK alive forever.
func TestKekCacheEvictsExpiredEntries(t *testing.T) {
	f := NewCryptoFactory(func(KmsConnectionConfig) (KmsClient, error) {
		return evictKMS{}, nil
	}, WithKekCacheLifetime(time.Nanosecond))
	cfg := EncryptionConfiguration{FooterKeyID: "footer_master", UniformEncryption: true}

	tokenA := KmsConnectionConfig{KmsInstanceID: "in-memory", KeyAccessToken: "token-a"}
	_, err := f.FileEncryptionProperties(tokenA, cfg)
	require.NoError(t, err)

	// the entry expires; a different token adds a new cache entry, which
	// is the sweep trigger
	time.Sleep(2 * time.Millisecond)
	tokenB := KmsConnectionConfig{KmsInstanceID: "in-memory", KeyAccessToken: "token-b"}
	_, err = f.FileEncryptionProperties(tokenB, cfg)
	require.NoError(t, err)

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Len(t, f.keks, 1, "the expired token-a entry must be swept")
	_, ok := f.keks[kekReuseKey{kmsInstanceID: "in-memory", accessToken: "token-b", masterKeyID: "footer_master"}]
	assert.True(t, ok, "the fresh token-b entry must survive the sweep")
}

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
	"container/list"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// KmsClient wraps and unwraps data encryption keys with master keys managed
// by a key management service. It mirrors the parquet-cpp/pyarrow KmsClient
// contract: implementations are provided by users, and the wrapped key
// representation is opaque to the library. Implementations must be safe for
// concurrent use.
type KmsClient interface {
	// WrapKey encrypts key with the master key identified by masterKeyID and
	// returns the KMS-specific wrapped representation.
	WrapKey(key []byte, masterKeyID string) (string, error)

	// UnwrapKey reverses WrapKey, returning the raw key bytes.
	UnwrapKey(wrappedKey string, masterKeyID string) ([]byte, error)
}

// KmsKeyRetriever implements parquet.DecryptionKeyRetriever for files whose
// footer and column key metadata carry PKMT1 key material (internal key
// storage), as written by the parquet-cpp/pyarrow CryptoFactory.
//
// In double wrapping mode the retriever caches unwrapped key encryption
// keys, so reading a file costs one KMS call per KEK instead of one per
// encrypted column plus the footer, matching parquet-cpp's KEK cache.
// Entries expire after kekCacheTTL and the cache holds at most
// kekCacheCapacity keys.
//
// A retriever wraps a single KmsClient identity, so share it only among
// callers with the same KMS access; parquet-cpp scopes its cache per
// access token instead.
type KmsKeyRetriever struct {
	client KmsClient
	keks   *kekCache
}

// NewKmsKeyRetriever returns a retriever that resolves keys through client.
// The retriever is safe for concurrent use and meant to be shared across
// files read with the same KMS access.
func NewKmsKeyRetriever(client KmsClient) *KmsKeyRetriever {
	return &KmsKeyRetriever{
		client: client,
		keks:   newKekCache(kekCacheTTL, kekCacheCapacity),
	}
}

// GetKey implements parquet.DecryptionKeyRetriever. Parse and unwrap errors
// surface as an empty key, which the reader reports as a missing key; use
// UnwrapKeyMetadata when the underlying error is needed.
func (r *KmsKeyRetriever) GetKey(keyMetadata []byte) string {
	key, err := r.UnwrapKeyMetadata(keyMetadata)
	if err != nil {
		return ""
	}
	return string(key)
}

// UnwrapKeyMetadata parses PKMT1 key metadata and returns the data
// encryption key it carries:
//
//   - single wrapping: the wrapped DEK is unwrapped by the KmsClient with
//     the master key;
//   - double wrapping: the KmsClient first unwraps the key encryption key
//     with the master key, and the DEK is then decrypted locally with
//     AES-GCM, using the KEK as the key and the (base64 decoded) KEK ID
//     bytes as the AAD, matching parquet-cpp's EncryptKeyLocally format:
//     nonce || ciphertext || tag.
func (r *KmsKeyRetriever) UnwrapKeyMetadata(keyMetadata []byte) ([]byte, error) {
	material, err := ParseKeyMetadata(keyMetadata)
	if err != nil {
		return nil, err
	}
	if !material.DoubleWrapping {
		dek, err := r.client.UnwrapKey(material.WrappedDEK, material.MasterKeyID)
		if err != nil {
			return nil, fmt.Errorf("parquet: unwrap data encryption key: %w", err)
		}
		return dek, nil
	}

	kek, err := r.keks.unwrap(kekCacheKey{material.MasterKeyID, material.KeyEncryptionKeyID}, func() ([]byte, error) {
		return r.client.UnwrapKey(material.WrappedKEK, material.MasterKeyID)
	})
	if err != nil {
		return nil, fmt.Errorf("parquet: unwrap key encryption key: %w", err)
	}

	// parquet-cpp base64 encodes the KEK ID in the key material but uses the
	// decoded ID bytes as the AES-GCM AAD for the locally wrapped DEK.
	aad, err := base64.StdEncoding.DecodeString(material.KeyEncryptionKeyID)
	if err != nil {
		return nil, fmt.Errorf("parquet: decode key encryption key id: %w", err)
	}
	wrappedDEK, err := base64.StdEncoding.DecodeString(material.WrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("parquet: decode wrapped data encryption key: %w", err)
	}
	dek, err := decryptKeyLocally(wrappedDEK, kek, aad)
	if err != nil {
		return nil, fmt.Errorf("parquet: decrypt data encryption key: %w", err)
	}
	return dek, nil
}

// decryptKeyLocally reverses parquet-cpp's EncryptKeyLocally: AES-GCM with a
// 12-byte nonce prefix and no length framing (nonce || ciphertext || tag).
func decryptKeyLocally(wrapped, key, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(wrapped) < gcm.NonceSize()+gcm.Overhead() {
		return nil, fmt.Errorf("wrapped key too short (%d bytes)", len(wrapped))
	}
	return gcm.Open(nil, wrapped[:gcm.NonceSize()], wrapped[gcm.NonceSize():], aad)
}

// kekCacheTTL bounds how long an unwrapped KEK survives a key rotation
// unnoticed; kekCacheCapacity bounds memory for long-lived retrievers that
// read many files, each carrying its own KEK.
const (
	kekCacheTTL      = 10 * time.Minute
	kekCacheCapacity = 64
)

// kekCacheKey identifies a KEK by the master key that wrapped it and the KEK
// ID carried in the key material, so KEKs from different master keys can
// never collide.
type kekCacheKey struct {
	masterKeyID        string
	keyEncryptionKeyID string
}

type kekCacheEntry struct {
	key       kekCacheKey
	kek       []byte
	expiresAt time.Time
	element   *list.Element // recency, list front = most recently used
}

// kekCache caches unwrapped KEKs (expiry plus LRU eviction) and collapses
// concurrent misses for the same key into one call.
type kekCache struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int
	now      func() time.Time
	entries  map[kekCacheKey]*kekCacheEntry
	recency  list.List
	inflight singleflight.Group
}

func newKekCache(ttl time.Duration, capacity int) *kekCache {
	return &kekCache{
		ttl:      ttl,
		capacity: capacity,
		now:      time.Now,
		entries:  make(map[kekCacheKey]*kekCacheEntry),
	}
}

func (c *kekCache) flightID(k kekCacheKey) string {
	// %q escaping keeps the mapping injective: a plain separator can be
	// ambiguous when an ID itself contains it
	return fmt.Sprintf("%q\x00%q", k.masterKeyID, k.keyEncryptionKeyID)
}

// get returns the cached KEK for key, or nil when absent or expired.
func (c *kekCache) get(k kekCacheKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[k]
	if !ok {
		return nil
	}
	if c.now().After(entry.expiresAt) {
		c.removeLocked(entry)
		return nil
	}
	c.recency.MoveToFront(entry.element)
	return entry.kek
}

// put caches kek for key, replacing any previous entry. The KEK is cloned:
// a KmsClient may reuse or zero the buffer it returned.
func (c *kekCache) put(k kekCacheKey, kek []byte) {
	kek = bytes.Clone(kek)

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	if entry, ok := c.entries[k]; ok {
		entry.kek = kek
		entry.expiresAt = now.Add(c.ttl)
		c.recency.MoveToFront(entry.element)
		return
	}
	// expired entries go first; only evict live ones when they alone fill
	// the cache
	for _, entry := range c.entries {
		if now.After(entry.expiresAt) {
			c.removeLocked(entry)
		}
	}
	for c.recency.Len() >= c.capacity {
		c.removeLocked(c.recency.Back().Value.(*kekCacheEntry))
	}
	entry := &kekCacheEntry{
		key:       k,
		kek:       kek,
		expiresAt: now.Add(c.ttl),
	}
	entry.element = c.recency.PushFront(entry)
	c.entries[k] = entry
}

// removeLocked drops entry from the map and the recency list; c.mu must be
// held.
func (c *kekCache) removeLocked(entry *kekCacheEntry) {
	delete(c.entries, entry.key)
	c.recency.Remove(entry.element)
}

// unwrap returns the cached KEK for key, calling unwrapKEK only on a miss.
// Concurrent misses for the same key share one unwrapKEK call.
func (c *kekCache) unwrap(k kekCacheKey, unwrapKEK func() ([]byte, error)) ([]byte, error) {
	if kek := c.get(k); kek != nil {
		return kek, nil
	}
	value, err, _ := c.inflight.Do(c.flightID(k), func() (any, error) {
		// the flight may have been queued behind another that already
		// filled the cache
		if kek := c.get(k); kek != nil {
			return kek, nil
		}
		kek, err := unwrapKEK()
		if err != nil {
			return nil, err
		}
		if len(kek) == 0 {
			return nil, fmt.Errorf("parquet: KMS returned an empty key encryption key for %q", k.masterKeyID)
		}
		c.put(k, kek)
		return kek, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

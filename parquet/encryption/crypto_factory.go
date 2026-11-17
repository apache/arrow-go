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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
)

// Key sizes for generated keys, matching the parquet-cpp/pyarrow
// CryptoFactory. Data key length is configurable via
// EncryptionConfiguration.DataKeyLengthBits; the KEK is always AES-128,
// as in parquet-cpp (kKekLength = 16).
const (
	kekSize     = 16
	kekIDSize   = 16
	defaultBits = 128
)

// DefaultKekCacheLifetime is how long a cached key encryption key is
// reused before it is wrapped again, matching parquet-cpp's
// cache_lifetime_seconds default of 600.
const DefaultKekCacheLifetime = 10 * time.Minute

// KmsClientFactory creates KmsClients for a connection configuration,
// mirroring parquet-cpp's KmsClientFactory.
type KmsClientFactory func(KmsConnectionConfig) (KmsClient, error)

// KmsConnectionConfig identifies the KMS instance and access credentials
// used for a file's key material. It is passed to the client factory and
// serialized into the footer key material, mirroring parquet-cpp/pyarrow.
type KmsConnectionConfig struct {
	KmsInstanceID  string
	KmsInstanceURL string
	KeyAccessToken string
}

// EncryptionConfiguration describes which master keys encrypt what in a
// file, mirroring parquet-cpp/pyarrow's EncryptionConfiguration.
type EncryptionConfiguration struct {
	// FooterKeyID is the master key id that wraps the footer key (and, in
	// uniform encryption, the single key encrypting every column). Required.
	FooterKeyID string

	// UniformEncryption encrypts every column with the footer key. Either
	// it or ColumnKeys must be set, never both — an empty ColumnKeys map
	// is a configuration error, not an implicit uniform layout (the
	// parquet-cpp rule).
	UniformEncryption bool

	// ColumnKeys maps a master key id to the column paths it encrypts; every
	// listed column gets its own data encryption key wrapped by that master
	// key. Columns not listed stay in plaintext.
	ColumnKeys map[string][]string

	// DataKeyLengthBits is the data encryption key length in bits: 128,
	// 192 or 256. Zero means 128, the parquet-cpp default.
	DataKeyLengthBits int

	// Algorithm picks the Parquet cipher: parquet.AesGcm (the zero value
	// and the parquet-cpp default) or parquet.AesGcmCtr.
	Algorithm parquet.Cipher

	// SingleWrapping wraps each data encryption key directly with its master
	// key. The default (false) is double wrapping (the parquet-cpp/pyarrow
	// default), where a per-master-key key encryption key wraps the data
	// keys locally and only it is wrapped by the KMS.
	SingleWrapping bool

	// AadPrefix is the AAD prefix identifier stored in the file (unless
	// DisableAadPrefixStorage is set) so readers can verify file identity.
	AadPrefix string

	// DisableAadPrefixStorage keeps the AadPrefix out of the file; readers
	// must then supply it themselves.
	DisableAadPrefixStorage bool

	// PlaintextFooter leaves the footer unencrypted (still authenticated
	// with a signature).
	PlaintextFooter bool
}

// CryptoFactory builds parquet.FileEncryptionProperties whose keys are
// wrapped by a key management service, producing the PKMT1 key material
// that KmsKeyRetriever (and parquet-cpp/pyarrow readers) resolve.
//
// In double wrapping mode the factory keeps one key encryption key per
// (KMS instance, key access token, master key) and reuses it across
// files, so writing many files costs one KEK wrap per master key per
// token — parquet-cpp's per-token KEK write cache, additionally scoped
// to the KMS instance so wraps are never shared across deployments.
// Entries expire after the cache lifetime (DefaultKekCacheLifetime,
// tunable with WithKekCacheLifetime) and are then wrapped again.
// Generation is serialized per cache entry, so files under different
// master keys wrap concurrently; only writers sharing a KEK wait for
// one another.
type CryptoFactory struct {
	clientFactory KmsClientFactory
	kekLifetime   time.Duration

	mu   sync.Mutex // guards the keks map only
	keks map[kekReuseKey]*cachedKek
}

// kekReuseKey identifies the KEK cached for a master key under one KMS
// instance and key access token: the token separates tenants the KMS
// authenticates, the instance keeps wraps from being shared across KMS
// deployments that may bind the same master key id to different keys.
type kekReuseKey struct {
	kmsInstanceID string
	accessToken   string
	masterKeyID   string
}

// cachedKek is a generated key encryption key together with its KMS-wrapped
// form, ready to be stamped into key material. Its mutex serializes
// (re)generation so the KMS call is made once per entry.
type cachedKek struct {
	mu        sync.Mutex
	createdAt time.Time

	id      []byte // raw KEK id; also the AAD for local data key wrapping
	kek     []byte
	wrapped string // KmsClient-wrapped KEK
}

// A CryptoFactoryOption configures a CryptoFactory.
type CryptoFactoryOption func(*CryptoFactory)

// WithKekCacheLifetime sets how long cached key encryption keys are
// reused before being wrapped again.
func WithKekCacheLifetime(d time.Duration) CryptoFactoryOption {
	return func(f *CryptoFactory) { f.kekLifetime = d }
}

// NewCryptoFactory returns a factory that creates KmsClients with
// clientFactory. A single factory can encrypt many files; call
// FileEncryptionProperties once per file, since the returned properties are
// single use (the parquet writer wipes their keys).
func NewCryptoFactory(clientFactory KmsClientFactory, opts ...CryptoFactoryOption) *CryptoFactory {
	f := &CryptoFactory{clientFactory: clientFactory}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// FileEncryptionProperties generates fresh data encryption keys for the
// footer and the configured columns, wraps them through the KMS client, and
// returns file encryption properties carrying the PKMT1 key material in the
// footer and column key metadata.
func (f *CryptoFactory) FileEncryptionProperties(kmsCfg KmsConnectionConfig,
	cfg EncryptionConfiguration) (*parquet.FileEncryptionProperties, error) {
	if cfg.FooterKeyID == "" {
		return nil, fmt.Errorf("parquet: encryption configuration needs a footer key id")
	}
	if cfg.UniformEncryption && len(cfg.ColumnKeys) > 0 {
		return nil, fmt.Errorf("parquet: cannot set both uniform encryption and column keys")
	}
	if !cfg.UniformEncryption && len(cfg.ColumnKeys) == 0 {
		return nil, fmt.Errorf("parquet: either column keys or uniform encryption must be set")
	}
	bits := cfg.DataKeyLengthBits
	if bits == 0 {
		bits = defaultBits
	}
	if bits != 128 && bits != 192 && bits != 256 {
		return nil, fmt.Errorf("parquet: data key length must be 128, 192 or 256 bits, got %d", bits)
	}
	if cfg.Algorithm != parquet.AesGcm && cfg.Algorithm != parquet.AesCtr {
		return nil, fmt.Errorf("parquet: encryption algorithm must be parquet.AesGcm or parquet.AesCtr, got %d", cfg.Algorithm)
	}
	keyBytes := bits / 8
	if f.clientFactory == nil {
		return nil, fmt.Errorf("parquet: CryptoFactory needs a client factory, construct it with NewCryptoFactory")
	}
	if cfg.DisableAadPrefixStorage && cfg.AadPrefix == "" {
		return nil, fmt.Errorf("parquet: DisableAadPrefixStorage is set without an AadPrefix")
	}
	client, err := f.clientFactory(kmsCfg)
	if err != nil {
		return nil, fmt.Errorf("parquet: create kms client: %w", err)
	}

	footerDEK, err := randomKey(keyBytes)
	if err != nil {
		return nil, err
	}
	material, err := f.wrapDataKey(client, kmsCfg, cfg, footerDEK, cfg.FooterKeyID)
	if err != nil {
		return nil, err
	}
	material.IsFooterKey = true
	material.KmsInstanceID = kmsCfg.KmsInstanceID
	material.KmsInstanceURL = kmsCfg.KmsInstanceURL
	footerJSON, err := json.Marshal(material)
	if err != nil {
		return nil, err
	}

	opts := []parquet.EncryptOption{
		parquet.WithFooterKeyMetadata(string(footerJSON)),
		parquet.WithAlg(cfg.Algorithm),
	}

	cols := parquet.ColumnPathToEncryptionPropsMap{}
	for masterKeyID, paths := range cfg.ColumnKeys {
		if masterKeyID == "" {
			return nil, fmt.Errorf("parquet: column keys map needs a master key id")
		}
		for _, path := range paths {
			if path == "" {
				return nil, fmt.Errorf("parquet: column keys for master key %q contain an empty column path", masterKeyID)
			}
			dek, err := randomKey(keyBytes)
			if err != nil {
				return nil, err
			}
			material, err := f.wrapDataKey(client, kmsCfg, cfg, dek, masterKeyID)
			if err != nil {
				return nil, err
			}
			colJSON, err := json.Marshal(material)
			if err != nil {
				return nil, err
			}
			if _, exists := cols[path]; exists {
				return nil, fmt.Errorf("parquet: column %q is listed under more than one master key", path)
			}
			cols[path] = parquet.NewColumnEncryptionProperties(path,
				parquet.WithKey(string(dek)), parquet.WithKeyMetadata(string(colJSON)))
		}
	}
	if len(cols) > 0 {
		opts = append(opts, parquet.WithEncryptedColumns(cols))
	}

	if cfg.AadPrefix != "" {
		opts = append(opts, parquet.WithAadPrefix(cfg.AadPrefix))
		if cfg.DisableAadPrefixStorage {
			opts = append(opts, parquet.DisableAadPrefixStorage())
		}
	}
	if cfg.PlaintextFooter {
		opts = append(opts, parquet.WithPlaintextFooter())
	}
	return parquet.NewFileEncryptionProperties(string(footerDEK), opts...), nil
}

// randomKey returns n cryptographically random bytes.
func randomKey(n int) ([]byte, error) {
	key := make([]byte, n)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("parquet: generate key: %w", err)
	}
	return key, nil
}

// wrapDataKey wraps dek with the master key masterKeyID and returns the
// PKMT1 key material for it.
func (f *CryptoFactory) wrapDataKey(client KmsClient, kmsCfg KmsConnectionConfig,
	cfg EncryptionConfiguration, dek []byte, masterKeyID string) (KeyMaterial, error) {

	material := KeyMaterial{
		KeyMaterialType: keyMaterialType1,
		InternalStorage: true,
		MasterKeyID:     masterKeyID,
	}
	if cfg.SingleWrapping {
		wrapped, err := client.WrapKey(dek, masterKeyID)
		if err != nil {
			return KeyMaterial{}, fmt.Errorf("parquet: wrap data encryption key with master key %q: %w", masterKeyID, err)
		}
		material.WrappedDEK = wrapped
		return material, nil
	}

	// double wrapping: a key encryption key wraps data keys locally and only
	// it is wrapped by the KMS
	kek, err := f.kekFor(client, kmsCfg, masterKeyID)
	if err != nil {
		return KeyMaterial{}, err
	}
	wrappedDEK, err := wrapKeyLocally(dek, kek.kek, kek.id)
	if err != nil {
		return KeyMaterial{}, err
	}
	material.DoubleWrapping = true
	material.WrappedDEK = base64.StdEncoding.EncodeToString(wrappedDEK)
	material.KeyEncryptionKeyID = base64.StdEncoding.EncodeToString(kek.id)
	material.WrappedKEK = kek.wrapped
	return material, nil
}

// kekFor returns the cached key encryption key for masterKeyID under the
// config's key access token, generating and wrapping a fresh one on first
// use or once the cache lifetime has passed. Each cache entry has its own
// lock, so only writers sharing a KEK wait for its KMS wrap; the map lock
// is never held across a KMS call.
func (f *CryptoFactory) kekFor(client KmsClient, kmsCfg KmsConnectionConfig,
	masterKeyID string) (*cachedKek, error) {
	key := kekReuseKey{
		kmsInstanceID: kmsCfg.KmsInstanceID,
		accessToken:   kmsCfg.KeyAccessToken,
		masterKeyID:   masterKeyID,
	}

	f.mu.Lock()
	entry, ok := f.keks[key]
	if !ok {
		entry = &cachedKek{}
		if f.keks == nil {
			f.keks = make(map[kekReuseKey]*cachedKek)
		}
		f.keks[key] = entry
	}
	f.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()
	lifetime := f.kekLifetime
	if lifetime <= 0 {
		lifetime = DefaultKekCacheLifetime
	}
	if !entry.createdAt.IsZero() && time.Since(entry.createdAt) <= lifetime {
		return entry, nil
	}
	id, err := randomKey(kekIDSize)
	if err != nil {
		return nil, err
	}
	kekKey, err := randomKey(kekSize)
	if err != nil {
		return nil, err
	}
	wrapped, err := client.WrapKey(kekKey, masterKeyID)
	if err != nil {
		return nil, fmt.Errorf("parquet: wrap key encryption key with master key %q: %w", masterKeyID, err)
	}
	entry.id, entry.kek, entry.wrapped = id, kekKey, wrapped
	entry.createdAt = time.Now()
	return entry, nil
}

// wrapKeyLocally is parquet-cpp's EncryptKeyLocally: AES-GCM with a
// 12-byte nonce prefix and no length framing (nonce || ciphertext || tag),
// producing the format decryptKeyLocally unwinds.
func wrapKeyLocally(key, kek, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("parquet: generate nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, key, aad), nil
}

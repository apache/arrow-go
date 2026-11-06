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
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
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
type KmsKeyRetriever struct {
	client KmsClient
}

// NewKmsKeyRetriever returns a retriever that resolves keys through client.
func NewKmsKeyRetriever(client KmsClient) *KmsKeyRetriever {
	return &KmsKeyRetriever{client: client}
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

	kek, err := r.client.UnwrapKey(material.WrappedKEK, material.MasterKeyID)
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

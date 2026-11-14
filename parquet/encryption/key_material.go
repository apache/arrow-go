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
	"encoding/json"
	"fmt"
)

// keyMaterialType1 is the only key material type defined by the parquet key
// management tools specification ("parquet key management tools, version 1").
const keyMaterialType1 = "PKMT1"

// KeyMaterial is the PKMT1 key material carried in the key metadata of a
// parquet file written with envelope encryption. With internal key storage,
// the material is stored as a flat JSON object in the footer and column key
// metadata of the file.
type KeyMaterial struct {
	// KeyMaterialType must be "PKMT1".
	KeyMaterialType string `json:"keyMaterialType"`
	// InternalStorage reports whether the key material is stored inside the
	// parquet file. External key material stores are not supported.
	InternalStorage bool `json:"internalStorage"`
	// IsFooterKey reports whether this material belongs to the footer key.
	// Only footer key material carries KmsInstanceID and KmsInstanceURL.
	IsFooterKey bool `json:"isFooterKey"`
	// KmsInstanceID and KmsInstanceURL identify the KMS instance that wrote
	// the footer key material; they are omitted from column key material,
	// matching parquet-cpp.
	KmsInstanceID  string `json:"kmsInstanceID,omitempty"`
	KmsInstanceURL string `json:"kmsInstanceURL,omitempty"`
	// MasterKeyID identifies the master key used to wrap the key.
	MasterKeyID string `json:"masterKeyID"`
	// WrappedDEK is the base64 encoded wrapped data encryption key.
	WrappedDEK string `json:"wrappedDEK"`
	// DoubleWrapping reports whether the DEK is wrapped by a key encryption
	// key that is itself wrapped by the master key.
	DoubleWrapping bool `json:"doubleWrapping"`
	// KeyEncryptionKeyID is the base64 encoded key encryption key ID, set
	// only in double wrapping mode.
	KeyEncryptionKeyID string `json:"keyEncryptionKeyID,omitempty"`
	// WrappedKEK is the base64 encoded wrapped key encryption key, set only
	// in double wrapping mode.
	WrappedKEK string `json:"wrappedKEK,omitempty"`
}

// ParseKeyMetadata parses the key metadata JSON of a footer or column key.
// Only internal key storage (the parquet-cpp/pyarrow CryptoFactory default)
// is supported; external key material stores are rejected.
func ParseKeyMetadata(metadata []byte) (KeyMaterial, error) {
	var material KeyMaterial
	if err := json.Unmarshal(metadata, &material); err != nil {
		return KeyMaterial{}, fmt.Errorf("parquet: parse key metadata: %w", err)
	}
	if material.KeyMaterialType != keyMaterialType1 {
		return KeyMaterial{}, fmt.Errorf("parquet: unsupported key material type %q (want %q)",
			material.KeyMaterialType, keyMaterialType1)
	}
	if !material.InternalStorage {
		return KeyMaterial{}, fmt.Errorf("parquet: external key material storage is not supported")
	}
	if material.MasterKeyID == "" || material.WrappedDEK == "" {
		return KeyMaterial{}, fmt.Errorf("parquet: key metadata missing masterKeyID or wrappedDEK")
	}
	if material.DoubleWrapping && (material.KeyEncryptionKeyID == "" || material.WrappedKEK == "") {
		return KeyMaterial{}, fmt.Errorf("parquet: double wrapped key material missing keyEncryptionKeyID or wrappedKEK")
	}
	return material, nil
}

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

package encryption_test

import (
	"encoding/base64"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet/encryption"
)

// toyKMS is a stand-in for a real key management service client: the wrapped
// key representation is opaque to the library, so anything reversible works
// for demonstration.
type toyKMS struct{}

func (toyKMS) WrapKey(key []byte, masterKeyID string) (string, error) {
	return base64.StdEncoding.EncodeToString(key), nil
}

func (toyKMS) UnwrapKey(wrappedKey string, masterKeyID string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(wrappedKey)
}

// ExampleKmsKeyRetriever shows how to read files written by the
// parquet-cpp/pyarrow CryptoFactory: implement KmsClient, wrap it in a
// KmsKeyRetriever and set it on the decryption properties with
// parquet.WithKeyRetriever. This package handles the PKMT1 parsing and
// unwrapping that would otherwise need a custom retriever.
func ExampleKmsKeyRetriever() {
	retriever := encryption.NewKmsKeyRetriever(toyKMS{})

	// key metadata as found in the footer of a CryptoFactory-written file
	// (single wrapping mode)
	metadata := []byte(`{
		"keyMaterialType": "PKMT1",
		"internalStorage": true,
		"isFooterKey": true,
		"kmsInstanceID": "kms-instance",
		"kmsInstanceURL": "https://kms.example",
		"masterKeyID": "footer_master",
		"wrappedDEK": "c2VjcmV0LWRlaw==",
		"doubleWrapping": false
	}`)

	fmt.Printf("%q\n", retriever.GetKey(metadata))

	// To open an encrypted file, pass the retriever to the reader:
	//
	//	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	//	props.FileDecryptProps = parquet.NewFileDecryptionProperties(
	//		parquet.WithKeyRetriever(retriever))
	//	fr, err := file.OpenParquetFile(path, false, file.WithReadProps(props))
	//
	// Code that does not import this package is unaffected.

	// Output: "secret-dek"
}

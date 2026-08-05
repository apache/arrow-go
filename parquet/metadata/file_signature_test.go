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

package metadata_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/internal/encryption"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/stretchr/testify/require"
)

func TestVerifySignatureRejectsTruncatedSignature(t *testing.T) {
	props := parquet.NewFileDecryptionProperties(parquet.WithFooterKey("0123456789abcdef"))
	meta := &metadata.FileMetaData{
		FileDecryptor: encryption.NewFileDecryptor(props, "", parquet.AesGcm, "", memory.DefaultAllocator),
	}

	for size := 0; size < encryption.NonceLength+encryption.GcmTagLength; size++ {
		if meta.VerifySignature(make([]byte, size)) {
			t.Fatalf("truncated signature of length %d was accepted", size)
		}
	}
	require.NotPanics(t, func() {
		require.False(t, meta.VerifySignature(make([]byte, encryption.NonceLength+encryption.GcmTagLength)))
	})
}

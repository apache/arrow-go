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

package encryption_test

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/encryption"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingBase64KMS mirrors the test KMS client of the interop harness:
// wrapping is plain base64. It counts calls so tests can assert how many
// KMS round trips key material generation costs.
type countingBase64KMS struct {
	wraps   atomic.Int32
	unwraps atomic.Int32
}

func (k *countingBase64KMS) WrapKey(key []byte, masterKeyID string) (string, error) {
	k.wraps.Add(1)
	return base64.StdEncoding.EncodeToString(key), nil
}

func (k *countingBase64KMS) UnwrapKey(wrappedKey string, masterKeyID string) ([]byte, error) {
	k.unwraps.Add(1)
	return base64.StdEncoding.DecodeString(wrappedKey)
}

func countingFactory(t *testing.T, kms *countingBase64KMS) *encryption.CryptoFactory {
	t.Helper()
	return encryption.NewCryptoFactory(func(encryption.KmsConnectionConfig) (encryption.KmsClient, error) {
		return kms, nil
	})
}

var testKmsConfig = encryption.KmsConnectionConfig{
	KmsInstanceID:  "in-memory",
	KmsInstanceURL: "local",
}

// In uniform encryption every column is encrypted with the footer key, so
// only the footer key material is generated. Single wrapping hands each data
// encryption key straight to the KMS client.
func TestFileEncryptionPropertiesUniformSingleWrapping(t *testing.T) {
	kms := &countingBase64KMS{}
	factory := countingFactory(t, kms)

	props, err := factory.FileEncryptionProperties(testKmsConfig,
		encryption.EncryptionConfiguration{
			FooterKeyID:    "footer_master",
			SingleWrapping: true,
		})
	require.NoError(t, err)

	// the footer key is a fresh 16-byte data key
	assert.Len(t, props.FooterKey(), 16)

	material, err := encryption.ParseKeyMetadata([]byte(props.FooterKeyMetadata()))
	require.NoError(t, err)
	assert.Equal(t, "footer_master", material.MasterKeyID)
	assert.True(t, material.IsFooterKey)
	assert.False(t, material.DoubleWrapping)
	assert.Equal(t, "in-memory", material.KmsInstanceID)
	assert.Equal(t, "local", material.KmsInstanceURL)

	// the retriever must recover the footer key from the metadata alone
	key, err := encryption.NewKmsKeyRetriever(kms).UnwrapKeyMetadata([]byte(props.FooterKeyMetadata()))
	require.NoError(t, err)
	assert.Equal(t, props.FooterKey(), string(key))

	// uniform encryption: no column properties, columns fall back to the
	// footer key
	assert.Empty(t, props.EncryptedColumns())
	assert.True(t, props.ColumnEncryptionProperties("a").IsEncryptedWithFooterKey())

	// one DEK wrapped directly by the KMS
	assert.Equal(t, int32(1), kms.wraps.Load())
}

// Double wrapping wraps only a per-master-key key encryption key with the
// KMS; data keys are wrapped locally. The KEK is reused across files, so a
// second properties object costs no extra KMS call (the pyarrow behavior).
func TestFileEncryptionPropertiesDoubleWrapping(t *testing.T) {
	kms := &countingBase64KMS{}
	factory := countingFactory(t, kms)
	cfg := encryption.EncryptionConfiguration{FooterKeyID: "footer_master"}

	props, err := factory.FileEncryptionProperties(testKmsConfig, cfg)
	require.NoError(t, err)
	assert.Len(t, props.FooterKey(), 16)

	material, err := encryption.ParseKeyMetadata([]byte(props.FooterKeyMetadata()))
	require.NoError(t, err)
	assert.True(t, material.DoubleWrapping)
	assert.True(t, material.IsFooterKey)
	assert.Equal(t, "footer_master", material.MasterKeyID)

	// the wrapped DEK carries the AES-GCM framing: 12 byte nonce + 16 byte
	// key + 16 byte tag
	wrappedDEK, err := base64.StdEncoding.DecodeString(material.WrappedDEK)
	require.NoError(t, err)
	assert.Len(t, wrappedDEK, 44)
	kekID, err := base64.StdEncoding.DecodeString(material.KeyEncryptionKeyID)
	require.NoError(t, err)
	assert.Len(t, kekID, 16)

	// the retriever must recover the footer key from the metadata alone,
	// unwrapping the KEK with the KMS and the DEK locally
	key, err := encryption.NewKmsKeyRetriever(kms).UnwrapKeyMetadata([]byte(props.FooterKeyMetadata()))
	require.NoError(t, err)
	assert.Equal(t, props.FooterKey(), string(key))
	assert.Equal(t, int32(1), kms.unwraps.Load(), "one KMS call to unwrap the KEK")

	// only the KEK is wrapped by the KMS, and it is reused for the next file
	_, err = factory.FileEncryptionProperties(testKmsConfig, cfg)
	require.NoError(t, err)
	assert.Equal(t, int32(1), kms.wraps.Load(), "KEK must be reused across files")

	// a fresh factory wraps a fresh KEK
	other := &countingBase64KMS{}
	_, err = countingFactory(t, other).FileEncryptionProperties(testKmsConfig, cfg)
	require.NoError(t, err)
	assert.Equal(t, int32(1), other.wraps.Load())
}

// With column keys every listed column gets its own data encryption key
// wrapped by its master key; columns under the same master key share the
// key encryption key, and unlisted columns stay in plaintext.
func TestFileEncryptionPropertiesColumnKeys(t *testing.T) {
	kms := &countingBase64KMS{}
	factory := countingFactory(t, kms)
	retriever := encryption.NewKmsKeyRetriever(kms)

	cfg := encryption.EncryptionConfiguration{
		FooterKeyID: "footer_master",
		ColumnKeys:  map[string][]string{"column_master": {"double_field", "float_field"}},
	}
	props, err := factory.FileEncryptionProperties(testKmsConfig, cfg)
	require.NoError(t, err)

	for _, path := range []string{"double_field", "float_field"} {
		col := props.ColumnEncryptionProperties(path)
		require.NotNil(t, col, path)
		assert.True(t, col.IsEncrypted(), path)
		assert.False(t, col.IsEncryptedWithFooterKey(), path)
		assert.Len(t, col.Key(), 16, path)

		material, err := encryption.ParseKeyMetadata([]byte(col.KeyMetadata()))
		require.NoError(t, err, path)
		assert.Equal(t, "column_master", material.MasterKeyID, path)
		assert.False(t, material.IsFooterKey, path)

		// column key material carries no KMS instance fields, matching
		// parquet-cpp
		assert.NotContains(t, col.KeyMetadata(), "kmsInstanceID", path)

		key, err := retriever.UnwrapKeyMetadata([]byte(col.KeyMetadata()))
		require.NoError(t, err, path)
		assert.Equal(t, col.Key(), string(key), path)
	}

	// every column gets a fresh data key, but both share the column master
	// key's KEK, distinct from the footer KEK
	a := props.ColumnEncryptionProperties("double_field")
	b := props.ColumnEncryptionProperties("float_field")
	assert.NotEqual(t, a.Key(), b.Key())
	am, err := encryption.ParseKeyMetadata([]byte(a.KeyMetadata()))
	require.NoError(t, err)
	bm, err := encryption.ParseKeyMetadata([]byte(b.KeyMetadata()))
	require.NoError(t, err)
	assert.Equal(t, am.KeyEncryptionKeyID, bm.KeyEncryptionKeyID)
	fm, err := encryption.ParseKeyMetadata([]byte(props.FooterKeyMetadata()))
	require.NoError(t, err)
	assert.NotEqual(t, am.KeyEncryptionKeyID, fm.KeyEncryptionKeyID)

	// unlisted columns stay in plaintext
	assert.Nil(t, props.ColumnEncryptionProperties("boolean_field"))

	// two master keys wrapped once each: the footer KEK and the column KEK
	assert.Equal(t, int32(2), kms.wraps.Load())
}

// Single wrapping with column keys wraps every data key with the KMS.
func TestFileEncryptionPropertiesColumnKeysSingleWrapping(t *testing.T) {
	kms := &countingBase64KMS{}
	factory := countingFactory(t, kms)
	retriever := encryption.NewKmsKeyRetriever(kms)

	props, err := factory.FileEncryptionProperties(testKmsConfig, encryption.EncryptionConfiguration{
		FooterKeyID:    "footer_master",
		ColumnKeys:     map[string][]string{"column_master": {"double_field", "float_field"}},
		SingleWrapping: true,
	})
	require.NoError(t, err)

	// footer + one data key per column: three KMS wraps
	assert.Equal(t, int32(3), kms.wraps.Load())

	col := props.ColumnEncryptionProperties("double_field")
	key, err := retriever.UnwrapKeyMetadata([]byte(col.KeyMetadata()))
	require.NoError(t, err)
	assert.Equal(t, col.Key(), string(key))
}

func TestFileEncryptionPropertiesErrors(t *testing.T) {
	factory := countingFactory(t, &countingBase64KMS{})

	_, err := factory.FileEncryptionProperties(testKmsConfig,
		encryption.EncryptionConfiguration{FooterKeyID: ""})
	assert.ErrorContains(t, err, "footer key id")

	_, err = factory.FileEncryptionProperties(testKmsConfig, encryption.EncryptionConfiguration{
		FooterKeyID: "footer_master",
		ColumnKeys:  map[string][]string{"": {"a"}},
	})
	assert.ErrorContains(t, err, "master key id")

	_, err = factory.FileEncryptionProperties(testKmsConfig, encryption.EncryptionConfiguration{
		FooterKeyID: "footer_master",
		ColumnKeys:  map[string][]string{"column_master": {"a", ""}},
	})
	assert.ErrorContains(t, err, "column path")

	// a column listed under two master keys would otherwise win
	// nondeterministically (map iteration order)
	_, err = factory.FileEncryptionProperties(testKmsConfig, encryption.EncryptionConfiguration{
		FooterKeyID: "footer_master",
		ColumnKeys: map[string][]string{
			"column_master": {"a"},
			"other_master":  {"a"},
		},
	})
	assert.ErrorContains(t, err, "more than one master key")

	// parquet-cpp rejects the combination too
	_, err = factory.FileEncryptionProperties(testKmsConfig, encryption.EncryptionConfiguration{
		FooterKeyID:             "footer_master",
		DisableAadPrefixStorage: true,
	})
	assert.ErrorContains(t, err, "DisableAadPrefixStorage")
}

// writeKmsEncryptedFile writes a file with an encrypted float64 column and a
// plaintext boolean column, using keys from the CryptoFactory.
func writeKmsEncryptedFile(t *testing.T, path string,
	props *parquet.FileEncryptionProperties) {
	t.Helper()

	fields := schema.FieldList{
		schema.Must(schema.NewPrimitiveNode("double_field", parquet.Repetitions.Required,
			parquet.Types.Double, 0, -1)),
		schema.NewBooleanNode("boolean_field", parquet.Repetitions.Required, -1),
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
	require.NoError(t, err)

	out, err := os.Create(path)
	require.NoError(t, err)

	writerProps := parquet.NewWriterProperties(parquet.WithEncryptionProperties(props))
	w := file.NewParquetWriter(out, root, file.WithWriterProps(writerProps))

	rgr, err := w.AppendBufferedRowGroupChecked()
	require.NoError(t, err)

	dw, err := rgr.Column(0)
	require.NoError(t, err)
	doubleWriter := dw.(*file.Float64ColumnChunkWriter)
	for i := 0; i < 3; i++ {
		_, err := doubleWriter.WriteBatch([]float64{float64(i) * 10}, nil, nil)
		require.NoError(t, err)
	}

	bw, err := rgr.Column(1)
	require.NoError(t, err)
	boolWriter := bw.(*file.BooleanColumnChunkWriter)
	for i := 0; i < 3; i++ {
		_, err := boolWriter.WriteBatch([]bool{i%2 == 0}, nil, nil)
		require.NoError(t, err)
	}

	require.NoError(t, w.Close())
}

// A file written with CryptoFactory keys must read back through the KMS key
// retriever alone, with no explicit keys anywhere.
func TestCryptoFactoryWriteReadRoundTrip(t *testing.T) {
	configs := []struct {
		name            string
		singleWrapping  bool
		expectedUnwraps int32
	}{
		{"double_wrapping", false, 2},
		{"single_wrapping", true, 3},
	}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			kms := &countingBase64KMS{}
			factory := countingFactory(t, kms)

			props, err := factory.FileEncryptionProperties(testKmsConfig, encryption.EncryptionConfiguration{
				FooterKeyID:    "footer_master",
				ColumnKeys:     map[string][]string{"column_master": {"double_field", "boolean_field"}},
				SingleWrapping: config.singleWrapping,
			})
			require.NoError(t, err)

			path := filepath.Join(t.TempDir(), "kms_written.parquet")
			writeKmsEncryptedFile(t, path, props)

			readProps := parquet.NewReaderProperties(memory.DefaultAllocator)
			readProps.FileDecryptProps = parquet.NewFileDecryptionProperties(
				parquet.WithKeyRetriever(encryption.NewKmsKeyRetriever(kms)))

			fr, err := file.OpenParquetFile(path, false, file.WithReadProps(readProps))
			require.NoError(t, err)
			defer fr.Close()

			assert.Equal(t, int64(3), fr.MetaData().NumRows)

			rg := fr.RowGroup(0)
			d, err := rg.Column(0)
			require.NoError(t, err)
			doubleReader := d.(*file.Float64ColumnChunkReader)
			vals := make([]float64, 8)
			n, _, err := doubleReader.ReadBatch(8, vals, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(3), n)
			assert.Equal(t, []float64{0, 10, 20}, vals[:3])

			b, err := rg.Column(1)
			require.NoError(t, err)
			boolReader := b.(*file.BooleanColumnChunkReader)
			bools := make([]bool, 8)
			n, _, err = boolReader.ReadBatch(8, bools, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(3), n)
			assert.Equal(t, []bool{true, false, true}, bools[:3])

			assert.Equal(t, config.expectedUnwraps, kms.unwraps.Load(),
				"KMS unwrap calls while reading")
		})
	}
}

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

package file_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryptedFileCipherAndAAD(t *testing.T) {
	const footerKey = "0123456789012345"
	values := []int32{3, 7, 11}
	sc, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{schema.NewInt32Node("value", parquet.Repetitions.Required, -1)}, -1)
	require.NoError(t, err)
	for _, cipher := range []struct {
		name      string
		algorithm parquet.Cipher
	}{
		{"GCM", parquet.AesGcm}, {"CTR", parquet.AesCtr},
	} {
		for _, footer := range []struct {
			name        string
			plaintext   bool
			aad         string
			externalAAD bool
		}{
			{"encrypted", false, "", false},
			{"plaintext", true, "", false},
			{"plaintext_stored_aad", true, "test-file", false},
			{"plaintext_external_aad", true, "test-file", true},
		} {
			t.Run(fmt.Sprintf("%s/%s", cipher.name, footer.name), func(t *testing.T) {
				opts := []parquet.EncryptOption{parquet.WithAlg(cipher.algorithm), parquet.WithFooterKeyMetadata("footer-key")}
				if footer.plaintext {
					opts = append(opts, parquet.WithPlaintextFooter())
				}
				if footer.aad != "" {
					opts = append(opts, parquet.WithAadPrefix(footer.aad))
				}
				if footer.externalAAD {
					opts = append(opts, parquet.DisableAadPrefixStorage())
				}
				encryption := parquet.NewFileEncryptionProperties(footerKey, opts...)
				props := parquet.NewWriterProperties(parquet.WithEncryptionProperties(encryption), parquet.WithDictionaryDefault(false))
				var buf bytes.Buffer
				writer, err := file.NewParquetWriterWithError(&buf, sc, file.WithWriterProps(props))
				require.NoError(t, err)
				group, err := writer.AppendRowGroupChecked()
				require.NoError(t, err)
				column, err := group.NextColumn()
				require.NoError(t, err)
				n, err := column.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil)
				require.NoError(t, err)
				require.EqualValues(t, len(values), n)
				require.NoError(t, group.Close())
				require.NoError(t, writer.Close())

				if footer.plaintext {
					// Inspect the serialized footer, including the AAD storage policy.
					data := buf.Bytes()
					footerLength := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
					meta, err := metadata.NewFileMetaData(data[len(data)-8-footerLength:len(data)-8], nil)
					require.NoError(t, err)
					assert.Equal(t, encryption.Algorithm(), meta.EncryptionAlgorithm())
					assert.Equal(t, []byte("footer-key"), meta.GetFooterSigningKeyMetadata())
				}

				readerProps := func() *parquet.ReaderProperties {
					decryption := []parquet.FileDecryptionOption{parquet.WithFooterKey(footerKey)}
					if footer.externalAAD {
						decryption = append(decryption, parquet.WithDecryptAadPrefix(footer.aad))
					}
					p := parquet.NewReaderProperties(nil)
					p.FileDecryptProps = parquet.NewFileDecryptionProperties(decryption...)
					return p
				}
				reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(readerProps()))
				require.NoError(t, err)
				defer reader.Close()
				require.EqualValues(t, len(values), reader.NumRows())
				readColumn, err := reader.RowGroup(0).Column(0)
				require.NoError(t, err)
				defer readColumn.Close()
				actual := make([]int32, len(values))
				total, count, err := readColumn.(*file.Int32ColumnChunkReader).ReadBatch(int64(len(values)), actual, nil, nil)
				require.NoError(t, err)
				require.NoError(t, readColumn.Err())
				require.EqualValues(t, len(values), total)
				require.Equal(t, len(values), count)
				assert.Equal(t, values, actual)
				assert.False(t, readColumn.HasNext())
				require.NoError(t, readColumn.Err())

				if cipher.algorithm == parquet.AesCtr && footer.plaintext && footer.aad == "" {
					// CTR data pages still use a GCM signature for a plaintext footer.
					corrupted := bytes.Clone(buf.Bytes())
					corrupted[len(corrupted)-9] ^= 1
					_, err := file.NewParquetReader(bytes.NewReader(corrupted), file.WithReadProps(readerProps()))
					require.ErrorContains(t, err, "signature verification failed")
				}
			})
		}
	}
}

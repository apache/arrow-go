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

package parquet_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/encryption"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed encryption_interop_pyarrow.py
var interopPyarrowScript string

const interopFooterKey = "0123456789012345"

// interopPythonTimeout bounds each script invocation so a hung python
// process cannot stall the test binary until the global go test timeout.
const interopPythonTimeout = 60 * time.Second

// runInteropPython runs the embedded script with the given arguments and
// returns its stdout; stderr is included in the error on failure.
func runInteropPython(t *testing.T, args ...string) (string, error) {
	t.Helper()

	dir := t.TempDir()
	script := filepath.Join(dir, "interop.py")
	if err := os.WriteFile(script, []byte(interopPyarrowScript), 0o600); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), interopPythonTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "python3", append([]string{script}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%v\n%s", err, stderr.String())
	}
	return stdout.String(), nil
}

// interopPython runs the embedded script and fails the test when it exits
// non-zero.
func interopPython(t *testing.T, args ...string) string {
	t.Helper()

	out, err := runInteropPython(t, args...)
	if err != nil {
		t.Fatalf("python3 %s failed: %v", strings.Join(args, " "), err)
	}
	return out
}

// requirePyarrow skips the test when python3 or pyarrow is not installed,
// so the interop suite stays green on machines without a python setup.
// PARQUET_TEST_REQUIRE_PYARROW is set by the cgo python CI job (the only
// environment that installs pyarrow) and turns the skip into a failure so
// an image change cannot silently disable the tests.
func requirePyarrow(t *testing.T) {
	t.Helper()

	mustHavePyarrow := os.Getenv("PARQUET_TEST_REQUIRE_PYARROW") != ""
	if _, err := exec.LookPath("python3"); err != nil {
		if mustHavePyarrow {
			t.Fatalf("PARQUET_TEST_REQUIRE_PYARROW is set but python3 not found in PATH: %v", err)
		}
		t.Skip("python3 not found in PATH, skipping pyarrow interop tests")
	}
	if _, err := runInteropPython(t, "probe"); err != nil {
		if mustHavePyarrow {
			t.Fatalf("PARQUET_TEST_REQUIRE_PYARROW is set but pyarrow is not available: %v", err)
		}
		t.Skipf("pyarrow not available for python3, skipping interop tests: %v", err)
	}
}

func TestPyarrowWrittenEncryptionConfigs(t *testing.T) {
	requirePyarrow(t)

	dir := t.TempDir()
	interopPython(t, "gen", dir, interopFooterKey)

	configs := []struct {
		name            string
		decrypt         *parquet.FileDecryptionProperties
		plaintextFooter bool
	}{
		{
			"uniform",
			parquet.NewFileDecryptionProperties(parquet.WithFooterKey(interopFooterKey)),
			false,
		},
		{
			"ctr",
			parquet.NewFileDecryptionProperties(parquet.WithFooterKey(interopFooterKey)),
			false,
		},
		{
			"aadprefix",
			parquet.NewFileDecryptionProperties(parquet.WithFooterKey(interopFooterKey),
				parquet.WithDecryptAadPrefix("myspecialfile")),
			false,
		},
		{
			// footer itself stays in plaintext, its key still decrypts column data
			"plainfooter_direct",
			parquet.NewFileDecryptionProperties(parquet.WithFooterKey(interopFooterKey)),
			true,
		},
	}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			path := filepath.Join(dir, config.name+".parquet")

			if config.plaintextFooter {
				// an encrypted-footer file ends in the PARE1 magic instead,
				// so assert the footer really did stay plaintext
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				assert.Equal(t, "PAR1", string(raw[len(raw)-4:]),
					"footer magic must stay plaintext")
			}

			props := parquet.NewReaderProperties(memory.DefaultAllocator)
			props.FileDecryptProps = config.decrypt

			fr, err := file.OpenParquetFile(path, false,
				file.WithReadProps(props))
			require.NoError(t, err)
			defer fr.Close()

			assert.Equal(t, int64(3), fr.MetaData().NumRows)

			rg := fr.RowGroup(0)
			a, err := rg.Column(0)
			require.NoError(t, err)
			f64 := a.(*file.Float64ColumnChunkReader)
			vals := make([]float64, 8)
			n, _, err := f64.ReadBatch(8, vals, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(3), n)
			assert.Equal(t, []float64{1, 2, 3}, vals[:3])

			b, err := rg.Column(1)
			require.NoError(t, err)
			f32 := b.(*file.Float32ColumnChunkReader)
			f32vals := make([]float32, 8)
			n, _, err = f32.ReadBatch(8, f32vals, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(3), n)
			assert.Equal(t, []float32{4, 5, 6}, f32vals[:3])
		})
	}
}

func TestGoWrittenEncryptionReadByPyarrow(t *testing.T) {
	requirePyarrow(t)

	path := filepath.Join(t.TempDir(), "go_written.parquet")
	writeInteropEncryptedFile(t, path)

	out := interopPython(t, "read", path, interopFooterKey)
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) == 2 {
			got[parts[0]] = parts[1]
		}
	}

	assert.Equal(t, "1 0 1", got["boolean_field"])
	assert.Equal(t, "0 10 20", got["int32_field"])
}

// interopBase64KMS mirrors the Base64KmsClient in encryption_interop_pyarrow.py:
// wrapping is plain base64, so the interop tests need no real KMS.
type interopBase64KMS struct{}

func (interopBase64KMS) WrapKey(key []byte, masterKeyID string) (string, error) {
	return base64.StdEncoding.EncodeToString(key), nil
}

func (interopBase64KMS) UnwrapKey(wrappedKey string, masterKeyID string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(wrappedKey)
}

// TestPyarrowKmsWrittenConfigs reads files written by pyarrow's CryptoFactory
// with PKMT1 key material, in both double (the pyarrow default) and single
// wrapping mode. The footer key and the column key of field "a" are both
// resolved through the KMS retriever.
func TestPyarrowKmsWrittenConfigs(t *testing.T) {
	requirePyarrow(t)

	dir := t.TempDir()
	interopPython(t, "gen-kms", dir)

	for _, name := range []string{"kms_double", "kms_single"} {
		t.Run(name, func(t *testing.T) {
			props := parquet.NewReaderProperties(memory.DefaultAllocator)
			props.FileDecryptProps = parquet.NewFileDecryptionProperties(
				parquet.WithKeyRetriever(encryption.NewKmsKeyRetriever(interopBase64KMS{})))

			fr, err := file.OpenParquetFile(filepath.Join(dir, name+".parquet"), false,
				file.WithReadProps(props))
			require.NoError(t, err)
			defer fr.Close()

			assert.Equal(t, int64(3), fr.MetaData().NumRows)

			rg := fr.RowGroup(0)
			a, err := rg.Column(0)
			require.NoError(t, err)
			f64 := a.(*file.Float64ColumnChunkReader)
			vals := make([]float64, 8)
			n, _, err := f64.ReadBatch(8, vals, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(3), n)
			assert.Equal(t, []float64{1, 2, 3}, vals[:3])

			b, err := rg.Column(1)
			require.NoError(t, err)
			f32 := b.(*file.Float32ColumnChunkReader)
			f32vals := make([]float32, 8)
			n, _, err = f32.ReadBatch(8, f32vals, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(3), n)
			assert.Equal(t, []float32{4, 5, 6}, f32vals[:3])
		})
	}
}

// writeInteropEncryptedFile writes a small encrypted parquet file with a footer
// key for pyarrow to read back.
func writeInteropEncryptedFile(t *testing.T, path string) {
	t.Helper()

	fields := schema.FieldList{
		schema.NewBooleanNode("boolean_field", parquet.Repetitions.Required, -1),
		schema.Must(schema.NewPrimitiveNode("int32_field", parquet.Repetitions.Required,
			parquet.Types.Int32, 0, -1)),
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
	require.NoError(t, err)

	out, err := os.Create(path)
	require.NoError(t, err)

	encProps := parquet.NewFileEncryptionProperties(interopFooterKey)
	props := parquet.NewWriterProperties(parquet.WithEncryptionProperties(encProps))
	w := file.NewParquetWriter(out, root, file.WithWriterProps(props))

	rgr, err := w.AppendBufferedRowGroupChecked()
	require.NoError(t, err)

	bw, err := rgr.Column(0)
	require.NoError(t, err)
	boolWriter := bw.(*file.BooleanColumnChunkWriter)
	for i := 0; i < 3; i++ {
		_, err := boolWriter.WriteBatch([]bool{i%2 == 0}, nil, nil)
		require.NoError(t, err)
	}

	iw, err := rgr.Column(1)
	require.NoError(t, err)
	int32Writer := iw.(*file.Int32ColumnChunkWriter)
	for i := int32(0); i < 3; i++ {
		_, err := int32Writer.WriteBatch([]int32{i * 10}, nil, nil)
		require.NoError(t, err)
	}

	require.NoError(t, w.Close())
}

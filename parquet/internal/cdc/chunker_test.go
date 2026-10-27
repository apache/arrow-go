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

package cdc

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGearTables(t *testing.T) {
	tables := gearTables()
	assert.Equal(t, uint64(0xf09f35a563783945), tables[0][0])
	assert.Equal(t, uint64(0x0dcc5b3bc5ae410a), tables[0][1])
	assert.Equal(t, uint64(0xecfcba92fe5691a3), tables[1][0])
	assert.Equal(t, uint64(0x15ed3fca3107bef5), tables[3][128])
	assert.Equal(t, uint64(0x8df9314914f0857e), tables[7][255])
	assert.Same(t, tables, gearTables())
}

func TestCalculateMask(t *testing.T) {
	const Ki, Mi = 1024, 1024 * 1024
	tests := []struct {
		name     string
		min, max int64
		norm     int
		mask     uint64
	}{
		{"256Ki-1Mi-norm0", 256 * Ki, 1 * Mi, 0, 0xFFFE000000000000},
		{"256Ki-1Mi-norm1", 256 * Ki, 1 * Mi, 1, 0xFFFC000000000000},
		{"256Ki-1Mi-norm2", 256 * Ki, 1 * Mi, 2, 0xFFF8000000000000},
		{"256Ki-1Mi-norm3", 256 * Ki, 1 * Mi, 3, 0xFFF0000000000000},
		{"256Ki-1Mi-norm-1", 256 * Ki, 1 * Mi, -1, 0xFFFF000000000000},
		{"0-32-norm0", 0, 32, 0, 0x8000000000000000},
		{"0-64-norm0", 0, 64, 0, 0xC000000000000000},
		{"0-16-norm-1", 0, 16, -1, 0x8000000000000000},
		{"128-384-norm-59", 128, 384, -59, 0xFFFFFFFFFFFFFFFE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mask, err := calculateMask(tt.min, tt.max, tt.norm)
			require.NoError(t, err)
			assert.Equal(t, tt.mask, mask)
		})
	}

	errTests := []struct {
		name     string
		min, max int64
		norm     int
	}{
		{"0-16-norm0", 0, 16, 0},
		{"0-32-norm1", 0, 32, 1},
		{"128-384-norm-60", 128, 384, -60},
	}
	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := calculateMask(tt.min, tt.max, tt.norm)
			require.ErrorContains(t, err, "between 1 and 63")
		})
	}
}

func TestNewChunkerValidation(t *testing.T) {
	const Ki, Mi, Gi = 1024, 1024 * 1024, 1024 * 1024 * 1024
	valid := []parquet.CDCOptions{
		{MinChunkSize: 256 * Ki, MaxChunkSize: 1 * Mi, NormLevel: 0},
		{MinChunkSize: 0, MaxChunkSize: 32, NormLevel: 0},
		{MinChunkSize: 32, MaxChunkSize: 64, NormLevel: 0},
		{MinChunkSize: 1 * Mi, MaxChunkSize: 2 * Mi, NormLevel: 0},
		{MinChunkSize: 1 * Gi, MaxChunkSize: 2 * Gi, NormLevel: 0},
		{MinChunkSize: 1, MaxChunkSize: 65, NormLevel: 1},
		{MinChunkSize: 0, MaxChunkSize: 128, NormLevel: 2},
	}
	for _, opts := range valid {
		c, err := NewChunker(opts)
		require.NoError(t, err, "%+v", opts)
		assert.NotNil(t, c)
	}

	invalid := []parquet.CDCOptions{
		{MinChunkSize: 0, MaxChunkSize: -1, NormLevel: 0},
		{MinChunkSize: 1024, MaxChunkSize: 512, NormLevel: 0},
		{MinChunkSize: -1, MaxChunkSize: 0, NormLevel: 0},
		{MinChunkSize: 0, MaxChunkSize: 0, NormLevel: 0},
		{MinChunkSize: 0, MaxChunkSize: 16, NormLevel: 0},
		{MinChunkSize: -16, MaxChunkSize: -16, NormLevel: 0},
		{MinChunkSize: 16, MaxChunkSize: 0, NormLevel: 0},
		{MinChunkSize: 32, MaxChunkSize: 32, NormLevel: 0},
		{MinChunkSize: 32, MaxChunkSize: 48, NormLevel: 0},
		{MinChunkSize: 1, MaxChunkSize: -1, NormLevel: 1},
		{MinChunkSize: -1, MaxChunkSize: 1, NormLevel: 1},
		{MinChunkSize: 1, MaxChunkSize: 1, NormLevel: 1},
		{MinChunkSize: 1, MaxChunkSize: 32, NormLevel: 1},
		{MinChunkSize: 1, MaxChunkSize: 33, NormLevel: 1},
		{MinChunkSize: 0, MaxChunkSize: 123, NormLevel: 2},
	}
	for _, opts := range invalid {
		_, err := NewChunker(opts)
		require.Error(t, err, "%+v", opts)
	}
}

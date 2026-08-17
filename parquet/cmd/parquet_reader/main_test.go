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

package main

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/stretchr/testify/require"
)

type columnChunkStatsStub struct {
	set           bool
	stats         metadata.TypedStatistics
	statsSetErr   error
	statisticsErr error
}

func (s *columnChunkStatsStub) StatsSet() (bool, error) {
	return s.set, s.statsSetErr
}

func (s *columnChunkStatsStub) Statistics() (metadata.TypedStatistics, error) {
	return s.stats, s.statisticsErr
}

func TestReadColumnStatsReturnsStatsSetError(t *testing.T) {
	wantErr := errors.New("malformed statistics")
	reader := &columnChunkStatsStub{statsSetErr: wantErr}

	stats, set, err := readColumnStats(reader)

	require.ErrorIs(t, err, wantErr)
	require.False(t, set)
	require.Nil(t, stats)
}

func TestReadColumnStatsReturnsStatisticsError(t *testing.T) {
	wantErr := errors.New("statistics could not be decoded")
	reader := &columnChunkStatsStub{set: true, statisticsErr: wantErr}

	stats, set, err := readColumnStats(reader)

	require.ErrorIs(t, err, wantErr)
	require.True(t, set)
	require.Nil(t, stats)
}

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

//go:build assert

package cdc

import "fmt"

// assertValidChunks panics unless chunks are contiguous in levels and values,
// only the first chunk is empty, and they cover exactly numLevels levels and
// numValues value slots. It is only compiled in with the assert build tag.
func assertValidChunks(chunks []Chunk, numLevels, numValues int64) {
	var levelOffset, valueOffset int64
	for i, ch := range chunks {
		if ch.LevelOffset != levelOffset || ch.ValueOffset != valueOffset ||
			ch.NumValues < 0 || ch.NumValues > ch.NumLevels || ch.NumLevels < 0 || (i > 0 && ch.NumLevels == 0) {
			panic(fmt.Sprintf("cdc: invalid chunk %d: %+v", i, ch))
		}
		levelOffset += ch.NumLevels
		valueOffset += ch.NumValues
	}
	if levelOffset != numLevels || valueOffset != numValues {
		panic(fmt.Sprintf("cdc: chunks cover %d levels and %d values, want %d and %d", levelOffset, valueOffset, numLevels, numValues))
	}
}

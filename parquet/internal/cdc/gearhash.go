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
	"crypto/md5"
	"encoding/binary"
	"sync"
)

const numGearTables = 8

// gearTables returns the gear hash tables, computed once on first use.
// Entry [seed][n] is the first 8 bytes of md5(seed*64 ++ n*64) read big-endian,
// matching the hex prefix produced by the C++ generate_hash helper.
var gearTables = sync.OnceValue(func() *[numGearTables][256]uint64 {
	var tables [numGearTables][256]uint64
	var buf [128]byte
	for seed := range numGearTables {
		for n := range 256 {
			for i := range buf {
				if i < 64 {
					buf[i] = byte(seed)
				} else {
					buf[i] = byte(n)
				}
			}
			sum := md5.Sum(buf[:])
			tables[seed][n] = binary.BigEndian.Uint64(sum[:8])
		}
	}
	return &tables
})

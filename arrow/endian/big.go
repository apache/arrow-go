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

//go:build s390x
// +build s390x

package endian

import "math/bits"

const (
	IsBigEndian     = true
	NativeEndian    = BigEndian
	NonNativeEndian = LittleEndian
)

func FromLE[T uint16 | uint32 | uint64](x T) T {
	switch v := any(x).(type) {
	case uint16:
		return T(bits.Reverse16(v))
	case uint32:
		return T(bits.Reverse32(v))
	case uint64:
		return T(bits.Reverse64(v))
	}
	return x
}

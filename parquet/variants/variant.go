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

package variants

// Basic types
type BasicType int

const (
	BasicUndefined   BasicType = -1
	BasicPrimitive   BasicType = 0
	BasicShortString BasicType = 1
	BasicObject      BasicType = 2
	BasicArray       BasicType = 3
)

func (bt BasicType) String() string {
	switch bt {
	case BasicPrimitive:
		return "Primitive"
	case BasicShortString:
		return "ShortString"
	case BasicObject:
		return "Object"
	case BasicArray:
		return "Array"
	}
	return "Unknown"
}

// Function to get the Variant basic type from a provided value header
func BasicTypeFromHeader(hdr byte) BasicType {
	return BasicType(hdr & 0x3)
}

// Container to hold a marshaled Variant.
type MarshaledVariant struct {
	Metadata []byte
	Value    []byte
}

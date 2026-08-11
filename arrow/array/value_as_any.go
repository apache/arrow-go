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

package array

// valueAsAnyFromListLike builds a []any of native child values for one list slot.
func valueAsAnyFromListLike(a ListLike, i int) any {
	if a.IsNull(i) {
		return nil
	}

	start, end := a.ValueOffsets(i)
	vals := a.ListValues()
	out := make([]any, end-start)
	for j := start; j < end; j++ {
		out[j-start] = vals.ValueAsAny(int(j))
	}
	return out
}

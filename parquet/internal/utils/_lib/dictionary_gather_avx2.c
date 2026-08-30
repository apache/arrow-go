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

#include <immintrin.h>
#include <stdint.h>

void dictionary_gather_32_avx2(const uint32_t* dictionary, uint32_t* output,
                               const int32_t* indices, const int len) {
  int i = 0;
  for (; i + 8 <= len; i += 8) {
    const __m256i index =
        _mm256_loadu_si256((const __m256i*)(indices + i));
    const __m256i values = _mm256_i32gather_epi32(
        (const int*)dictionary, index, sizeof(uint32_t));
    _mm256_storeu_si256((__m256i*)(output + i), values);
  }

  for (; i < len; ++i) {
    output[i] = dictionary[indices[i]];
  }
}

void dictionary_gather_64_avx2(const uint64_t* dictionary, uint64_t* output,
                               const int32_t* indices, const int len) {
  int i = 0;
  for (; i + 4 <= len; i += 4) {
    const __m128i index =
        _mm_loadu_si128((const __m128i*)(indices + i));
    const __m256i values = _mm256_i32gather_epi64(
        (const long long*)dictionary, index, sizeof(uint64_t));
    _mm256_storeu_si256((__m256i*)(output + i), values);
  }

  for (; i < len; ++i) {
    output[i] = dictionary[indices[i]];
  }
}

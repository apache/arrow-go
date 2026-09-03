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

#include <arch.h>
#include <immintrin.h>
#include <stdint.h>

extern "C" void FULL_NAME(filter_uint32)(const uint32_t* values,
                                           const uint8_t* filter,
                                           uint32_t* output,
                                           const uint8_t* tables,
                                           const int64_t length) {
    const uint8_t* shuffle_masks = tables;
    const int32_t* store_masks = reinterpret_cast<const int32_t*>(tables + 256);
    const uint8_t* popcount = tables + 336;
    int64_t output_length = 0;
    const int64_t num_bytes = length / 8;

    for (int64_t i = 0; i < num_bytes; ++i) {
        const uint8_t mask = filter[i];
        const uint32_t* input = values + i * 8;

        if (mask == 0) {
            continue;
        }

        if (mask == 0xff) {
            const __m256i low = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + output_length), low);
            output_length += 8;
            continue;
        }

        const uint8_t low_mask = mask & 0xf;
        const uint8_t high_mask = mask >> 4;
        const int low_count = popcount[low_mask];
        const int high_count = popcount[high_mask];
        const __m128i low_values = _mm_loadu_si128(reinterpret_cast<const __m128i*>(input));
        const __m128i high_values = _mm_loadu_si128(reinterpret_cast<const __m128i*>(input + 4));

        if (low_count != 0) {
            const __m128i shuffle = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(shuffle_masks + low_mask * 16));
            const __m128i compacted = _mm_shuffle_epi8(low_values, shuffle);
            const __m128i store_mask = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(store_masks + low_count * 4));
            _mm_maskstore_epi32(reinterpret_cast<int*>(output + output_length), store_mask, compacted);
            output_length += low_count;
        }

        if (high_count != 0) {
            const __m128i shuffle = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(shuffle_masks + high_mask * 16));
            const __m128i compacted = _mm_shuffle_epi8(high_values, shuffle);
            const __m128i store_mask = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(store_masks + high_count * 4));
            _mm_maskstore_epi32(reinterpret_cast<int*>(output + output_length), store_mask, compacted);
            output_length += high_count;
        }
    }

}

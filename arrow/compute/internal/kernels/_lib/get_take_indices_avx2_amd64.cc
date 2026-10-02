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

extern "C" void FULL_NAME(get_take_indices_uint32)(const uint8_t* filter,
                                                     uint32_t* output,
                                                     const uint8_t* tables,
                                                     const int64_t nbytes,
                                                     const int64_t tail_mask) {
    const uint8_t* shuffle_masks = tables;
    const int32_t* store_masks = reinterpret_cast<const int32_t*>(tables + 256);
    const uint8_t* popcount = tables + 336;
    const __m128i offsets = _mm_setr_epi32(0, 1, 2, 3);
    const __m128i high_offset = _mm_set1_epi32(4);
    const __m128i increment = _mm_set1_epi32(8);
    __m128i base = _mm_setzero_si128();
    int64_t output_length = 0;

    for (int64_t i = 0; i < nbytes; ++i) {
        uint8_t mask = filter[i];
        if (i == nbytes - 1) {
            mask &= static_cast<uint8_t>(tail_mask);
        }

        if (mask == 0) {
            base = _mm_add_epi32(base, increment);
            continue;
        }

        if (mask == 0xff) {
            const __m128i low = _mm_add_epi32(base, offsets);
            const __m128i high = _mm_add_epi32(low, high_offset);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(output + output_length), low);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(output + output_length + 4), high);
            output_length += 8;
            base = _mm_add_epi32(base, increment);
            continue;
        }

        const uint8_t low_mask = mask & 0x0f;
        const uint8_t high_mask = mask >> 4;
        const int low_count = popcount[low_mask];
        const int high_count = popcount[high_mask];

        if (low_count != 0) {
            const __m128i shuffle = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(shuffle_masks + low_mask * 16));
            const __m128i values = _mm_add_epi32(base, offsets);
            const __m128i compacted = _mm_shuffle_epi8(values, shuffle);
            const __m128i store_mask = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(store_masks + low_count * 4));
            _mm_maskstore_epi32(reinterpret_cast<int*>(output + output_length), store_mask, compacted);
            output_length += low_count;
        }

        if (high_count != 0) {
            const __m128i shuffle = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(shuffle_masks + high_mask * 16));
            const __m128i values = _mm_add_epi32(_mm_add_epi32(base, offsets), high_offset);
            const __m128i compacted = _mm_shuffle_epi8(values, shuffle);
            const __m128i store_mask = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(store_masks + high_count * 4));
            _mm_maskstore_epi32(reinterpret_cast<int*>(output + output_length), store_mask, compacted);
            output_length += high_count;
        }

        base = _mm_add_epi32(base, increment);
    }
}

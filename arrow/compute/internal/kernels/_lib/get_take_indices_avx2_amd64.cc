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
    if (nbytes <= 0) {
        return;
    }

    const uint8_t* shuffle_masks = tables;
    const int32_t* store_masks = reinterpret_cast<const int32_t*>(tables + 256);
    const uint8_t* popcount = tables + 336;
    const __m128i offsets = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tables + 352));
    const __m128i high_offsets = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tables + 368));
    const __m128i increment = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tables + 384));
    __m128i base = _mm_setzero_si128();

    const auto compact = [&](uint8_t mask) {
        if (mask == 0) {
            base = _mm_add_epi32(base, increment);
            return;
        }

        if (mask == 0xff) {
            const __m128i low = _mm_or_si128(base, offsets);
            const __m128i high = _mm_or_si128(base, high_offsets);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(output), low);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(output + 4), high);
            output += 8;
            base = _mm_add_epi32(base, increment);
            return;
        }

        // Keep the nibble index wide and defer the high-half lookup. This
        // lowers register pressure so clang does not use BP as scratch state.
        const int low_mask = mask & 0x0f;
        const int low_count = popcount[low_mask];

        if (low_count != 0) {
            const __m128i shuffle = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(shuffle_masks + low_mask * 16));
            const __m128i values = _mm_or_si128(base, offsets);
            const __m128i compacted = _mm_shuffle_epi8(values, shuffle);
            const __m128i store_mask = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(store_masks + low_count * 4));
            _mm_maskstore_epi32(reinterpret_cast<int*>(output), store_mask, compacted);
            output += low_count;
        }

        const int high_mask = mask >> 4;
        const int high_count = popcount[high_mask];
        if (high_count != 0) {
            const __m128i shuffle = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(shuffle_masks + high_mask * 16));
            const __m128i values = _mm_or_si128(base, high_offsets);
            const __m128i compacted = _mm_shuffle_epi8(values, shuffle);
            const __m128i store_mask = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(store_masks + high_count * 4));
            _mm_maskstore_epi32(reinterpret_cast<int*>(output), store_mask, compacted);
            output += high_count;
        }

        base = _mm_add_epi32(base, increment);
    };

    // Only the last byte contains padding bits. Keep its mask out of the loop.
    for (int64_t i = 0; i < nbytes - 1; ++i) {
        compact(filter[i]);
    }
    compact(filter[nbytes - 1] & static_cast<uint8_t>(tail_mask));
}

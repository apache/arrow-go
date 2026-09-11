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

extern "C" void FULL_NAME(filter_uint64)(const uint64_t* values,
                                           const uint8_t* filter,
                                           uint64_t* output,
                                           const uint8_t* tables,
                                           const int64_t length) {
    const uint8_t* permutation_tables = tables;
    const uint64_t* store_masks = reinterpret_cast<const uint64_t*>(tables + 512);
    const uint8_t* popcount = tables + 672;
    int64_t output_length = 0;
    const int64_t num_bytes = length / 8;

    for (int64_t i = 0; i < num_bytes; ++i) {
        const uint8_t mask = filter[i];
        const uint64_t* input = values + i * 8;

        if (mask == 0) {
            continue;
        }

        if (mask == 0xff) {
            const __m256i low = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input));
            const __m256i high = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input + 4));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + output_length), low);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + output_length + 4), high);
            output_length += 8;
            continue;
        }

        const uint8_t low_mask = mask & 0xf;
        const uint8_t high_mask = mask >> 4;
        const int low_count = popcount[low_mask];
        const int high_count = popcount[high_mask];

        if (low_count != 0) {
            const __m256i input_values = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input));
            const __m256i permutation = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(permutation_tables + low_mask * 32));
            const __m256i compacted = _mm256_permutevar8x32_epi32(input_values, permutation);
            const __m256i store_mask = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(store_masks + low_count * 4));
            _mm256_maskstore_epi32(reinterpret_cast<int*>(output + output_length),
                                   store_mask, compacted);
            output_length += low_count;
        }

        if (high_count != 0) {
            const __m256i input_values = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input + 4));
            const __m256i permutation = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(permutation_tables + high_mask * 32));
            const __m256i compacted = _mm256_permutevar8x32_epi32(input_values, permutation);
            const __m256i store_mask = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(store_masks + high_count * 4));
            _mm256_maskstore_epi32(reinterpret_cast<int*>(output + output_length),
                                   store_mask, compacted);
            output_length += high_count;
        }
    }
}

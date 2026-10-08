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
#include <smmintrin.h>
#include <tmmintrin.h>
#include <stdint.h>

extern "C" void FULL_NAME(filter_uint8)(const uint8_t* values,
                                          const uint8_t* filter,
                                          uint8_t* output,
                                          const uint8_t* tables,
                                          const int64_t length) {
    const uint8_t* shuffle_masks = tables;
    const uint8_t* popcount = tables + 4096;
    int64_t output_length = 0;
    const int64_t num_bytes = length / 8;

    for (int64_t i = 0; i < num_bytes; ++i) {
        const uint8_t mask = filter[i];
        if (mask == 0) {
            continue;
        }

        const uint8_t* input = values + i * 8;
        if (mask == 0xff) {
            const __m128i input_values = _mm_loadl_epi64(
                reinterpret_cast<const __m128i*>(input));
            _mm_storel_epi64(reinterpret_cast<__m128i*>(output + output_length), input_values);
            output_length += 8;
            continue;
        }

        const int count = popcount[mask];
        const __m128i input_values = _mm_loadl_epi64(
            reinterpret_cast<const __m128i*>(input));
        const __m128i shuffle = _mm_loadu_si128(
            reinterpret_cast<const __m128i*>(shuffle_masks + mask * 16));
        const __m128i compacted = _mm_shuffle_epi8(input_values, shuffle);

        if (count >= 4) {
            _mm_storeu_si32(output + output_length, compacted);
            output_length += 4;
            if (count >= 7) {
                output[output_length + 2] = static_cast<uint8_t>(_mm_extract_epi8(compacted, 6));
            }
            if (count >= 6) {
                output[output_length + 1] = static_cast<uint8_t>(_mm_extract_epi8(compacted, 5));
            }
            if (count >= 5) {
                output[output_length] = static_cast<uint8_t>(_mm_extract_epi8(compacted, 4));
            }
            output_length += count - 4;
        } else {
            uint32_t packed = static_cast<uint32_t>(_mm_cvtsi128_si32(compacted));
            for (int lane = 0; lane < count; ++lane) {
                output[output_length++] = static_cast<uint8_t>(packed);
                packed >>= 8;
            }
        }
    }
}

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

#include <arm_neon.h>
#include <stdint.h>

static inline uint64x2_t multiply_u64x2_by_u64(const uint64x2_t values,
                                               const uint64_t factor) {
    const uint32x4_t values32 = vreinterpretq_u32_u64(values);
    const uint32x2_t values_lo =
        vuzp1_u32(vget_low_u32(values32), vget_high_u32(values32));
    const uint32x2_t values_hi =
        vuzp2_u32(vget_low_u32(values32), vget_high_u32(values32));
    const uint32x2_t factor_lo = vdup_n_u32((uint32_t)factor);
    const uint32x2_t factor_hi = vdup_n_u32((uint32_t)(factor >> 32));
    const uint64x2_t low = vmull_u32(values_lo, factor_lo);
    const uint64x2_t cross =
        vmlal_u32(vmull_u32(values_lo, factor_hi), values_hi, factor_lo);

    return vaddq_u64(low, vshlq_n_u64(cross, 32));
}

void multiply_constant_int32_int32_neon(const int32_t* src, int32_t* dest,
                                        const int len, const int64_t factor) {
    int i = 0;
    const uint32x4_t factor_vec = vdupq_n_u32((uint32_t)factor);

    for (; i + 4 <= len; i += 4) {
        const uint32x4_t values = vld1q_u32((const uint32_t*)(src + i));
        vst1q_u32((uint32_t*)(dest + i), vmulq_u32(values, factor_vec));
    }

    for (; i < len; ++i) {
        dest[i] = (int32_t)((uint32_t)src[i] * (uint32_t)factor);
    }
}

void multiply_constant_int32_int64_neon(const int32_t* src, int64_t* dest,
                                        const int len, const int64_t factor) {
    int i = 0;

    for (; i + 4 <= len; i += 4) {
        const int32x4_t values = vld1q_s32(src + i);
        const uint64x2_t low =
            vreinterpretq_u64_s64(vmovl_s32(vget_low_s32(values)));
        const uint64x2_t high =
            vreinterpretq_u64_s64(vmovl_s32(vget_high_s32(values)));
        vst1q_u64((uint64_t*)(dest + i), multiply_u64x2_by_u64(low, factor));
        vst1q_u64((uint64_t*)(dest + i + 2),
                  multiply_u64x2_by_u64(high, factor));
    }

    for (; i < len; ++i) {
        dest[i] = (int64_t)((uint64_t)(int64_t)src[i] * (uint64_t)factor);
    }
}

void multiply_constant_int64_int32_neon(const int64_t* src, int32_t* dest,
                                        const int len, const int64_t factor) {
    int i = 0;
    const uint32x4_t factor_vec = vdupq_n_u32((uint32_t)factor);

    for (; i + 4 <= len; i += 4) {
        const uint64x2_t low_values =
            vld1q_u64((const uint64_t*)(src + i));
        const uint64x2_t high_values =
            vld1q_u64((const uint64_t*)(src + i + 2));
        const uint32x4_t low_values32 = vreinterpretq_u32_u64(low_values);
        const uint32x4_t high_values32 = vreinterpretq_u32_u64(high_values);
        const uint32x2_t low =
            vuzp1_u32(vget_low_u32(low_values32), vget_high_u32(low_values32));
        const uint32x2_t high = vuzp1_u32(vget_low_u32(high_values32),
                                          vget_high_u32(high_values32));
        const uint32x4_t values = vcombine_u32(low, high);
        vst1q_u32((uint32_t*)(dest + i), vmulq_u32(values, factor_vec));
    }

    for (; i < len; ++i) {
        dest[i] = (int32_t)((uint32_t)src[i] * (uint32_t)factor);
    }
}

void multiply_constant_int64_int64_neon(const int64_t* src, int64_t* dest,
                                        const int len, const int64_t factor) {
    int i = 0;

    for (; i + 2 <= len; i += 2) {
        const uint64x2_t values = vld1q_u64((const uint64_t*)(src + i));
        vst1q_u64((uint64_t*)(dest + i), multiply_u64x2_by_u64(values, factor));
    }

    for (; i < len; ++i) {
        dest[i] = (int64_t)((uint64_t)src[i] * (uint64_t)factor);
    }
}

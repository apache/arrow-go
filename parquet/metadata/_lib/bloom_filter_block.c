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

#include "arch.h"
#include <stdint.h>
#include <stdbool.h>

// algorithms defined in https://github.com/apache/parquet-format/blob/master/BloomFilter.md
// describing the proper definitions for the bloom filter hash functions
// to be compatible with the parquet format

#define bitsSetPerBlock 8
static const uint32_t SALT[bitsSetPerBlock] = {
    0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
    0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

#define PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))

bool FULL_NAME(check_block)(const uint32_t blocks[], const int len, const uint64_t hash){
    const uint32_t bucket_index =
        (uint32_t)(((hash >> 32) * (uint64_t)(len/8)) >> 32);
    const uint32_t key = (uint32_t)hash;

    for (int i = 0; i < bitsSetPerBlock; ++i)
    {
        const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
        if (PREDICT_FALSE(0 == (blocks[bitsSetPerBlock * bucket_index + i] & mask)))
        {
            return false;
        }
    }
    return true;
}

void FULL_NAME(insert_block)(uint32_t blocks[], const int len, const uint64_t hash) {
    const uint32_t bucket_index =
        (uint32_t)(((hash >> 32) * (uint64_t)(len/8)) >> 32);
    const uint32_t key = (uint32_t)hash;

    for (int i = 0; i < bitsSetPerBlock; ++i)
    {
        const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
        blocks[bitsSetPerBlock * bucket_index + i] |= mask;
    }
}

void FULL_NAME(insert_bulk)(uint32_t blocks[], const int block_len, const uint64_t hashes[], const int num_hashes) {
    for (int i = 0; i < num_hashes; ++i) {
        FULL_NAME(insert_block)(blocks, block_len, hashes[i]);
    }
}
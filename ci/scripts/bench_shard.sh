#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Print a GitHub Actions matrix (JSON) that splits the packages containing Go
# benchmarks into up to <num_shards> groups, so bench.sh --run can execute them
# in parallel jobs. Each element is {"id":N,"packages":"./pkg-a ./pkg-b ..."}.

set -eo pipefail

num_shards="${1:-6}"
source_dir="${2:-.}"

cd "${source_dir}"

grep -rlE '^func Benchmark' --include='*_test.go' . |
  xargs -n1 dirname |
  sort -u |
  awk -v want="${num_shards}" '
      $0 == "" { next }
      { pkg = ($0 ~ /^\.\//) ? $0 : "./" $0; pkgs[++count] = pkg }
      END {
        if (count == 0) { print "[]"; exit }
        n = (want < count) ? want : count
        for (k = 1; k <= count; k++) {
          s = (k - 1) % n
          shard[s] = shard[s] (shard[s] == "" ? "" : " ") pkgs[k]
        }
        printf "["
        for (i = 0; i < n; i++) {
          if (i > 0) printf ","
          printf "{\"id\":%d,\"packages\":\"%s\"}", i, shard[i]
        }
        printf "]\n"
      }'

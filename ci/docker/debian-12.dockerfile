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

ARG arch=amd64
ARG go=1.24
FROM ${arch}/golang:${go}-bookworm

# ci/scripts/test.sh only runs -asan against an LLVM >= 19 runtime; the
# libsanitizer shipped with the image's GCC predates the thread-registry fix.
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        clang-19 \
        libclang-rt-19-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the go.mod and go.sum over and pre-download all the dependencies
COPY . /arrow-go
RUN cd /arrow-go && \
    for attempt in 1 2 3 4 5; do \
      go mod download github.com/apache/arrow-go/v18@latest && exit 0; \
      if [ "${attempt}" -eq 5 ]; then exit 1; fi; \
      sleep $((attempt * 2)); \
    done

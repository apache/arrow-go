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

set -eux

source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

arrow_source="${source_dir}/arrow-source"
if [ -d "${arrow_source}" ]; then
  have_arrow_source=true
else
  have_arrow_source=false
  git clone --depth 1 https://github.com/apache/arrow.git "${arrow_source}"
fi

pushd "${source_dir}/arrow/flight"
APACHE_ARROW_FORMAT_DIR="${arrow_source}/format" go generate gen.go
popd

if [ "${have_arrow_source}" = "false" ]; then
  rm -rf "${arrow_source}"
fi

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

source_dir=${1}

export PARQUET_TEST_DATA=${1}/parquet-testing/data
export PARQUET_TEST_BAD_DATA=${1}/parquet-testing/bad_data
export ARROW_TEST_DATA=${1}/arrow-testing/data

case "$(uname)" in
MINGW*)
  # -race and -asan don't work on Windows currently
  test_args=()
  ;;
*)
  if [[ "$(go env GOHOSTARCH)" = "s390x" ]]; then
    # -race and -asan not supported on s390x
    test_args=()
  else
    if [[ "$(go env GOOS)" = "darwin" ]]; then
      # -asan not supported on darwin/amd64
      test_args=("-race")
    else
      test_args=("-asan")
    fi
  fi
  ;;
esac

pushd "${source_dir}/arrow"

: "${ARROW_GO_TESTCGO:=}"

tags="assert,test"
if [[ -n "${ARROW_GO_TESTCGO}" ]]; then
  if [[ "${MSYSTEM:-}" = "MINGW64" ]]; then
    export PATH=${MINGW_PREFIX}\\bin:${MINGW_PREFIX}\\lib:$PATH
  fi

  if [[ "$(go env GOOS)" = "darwin" ]]; then
    # see https://github.com/golang/go/issues/61229#issuecomment-1988965927
    test_args+=("-ldflags=-extldflags=-Wl,-ld_classic")
  fi
  tags+=",ccalloc"
fi

# the cgo implementation of the c data interface requires the "test"
# tag in order to run its tests so that the testing functions implemented
# in .c files don't get included in non-test builds.

go test "${test_args[@]}" -short -tags ${tags} ./...

# run it again but with the noasm tag
go test "${test_args[@]}" -short -tags ${tags},noasm ./...

popd

pushd "${source_dir}/parquet"

parquet_test_args=("${test_args[@]}")
if [[ "$(go env GOOS)" = "darwin" ]]; then
  # Keep package-level memory bounded on the 7 GB macOS ARM64 runners.
  parquet_test_args+=("-p=1")
fi

go test "${parquet_test_args[@]}" -tags assert ./...

# run the tests again but with the noasm tag
go test "${parquet_test_args[@]}" -tags assert,noasm ./...

popd

# arrgen is a nested module: "./..." above stops at its go.mod, so it needs its
# own vet, test and generation check. Its allocation assertions step aside under
# -race and -asan on their own, via build tags.
pushd "${source_dir}/arrgen"

go vet ./...

go test "${test_args[@]}" ./...

# TestCheckedInFilesAreUpToDate covers the same ground through the arrgen
# package, but only running the committed go:generate directives covers the
# command wrapper and the flags they pass it.
go generate ./...
git diff --exit-code -- .

popd

# The run above resolves the released arrow-go that arrgen's go.mod names, which
# is what a consumer of the generator gets. Run it against this tree as well: the
# equivalence tests are what catch a change to arrow/array/arreflect that the
# generated encoders no longer match, and in module mode they would not see one
# until after the next release.
pushd "${source_dir}"

rm -f go.work go.work.sum
go work init . ./arrgen
go test "${test_args[@]}" ./arrgen/...
rm -f go.work go.work.sum

popd

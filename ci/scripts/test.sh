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

# Go's -asan links whatever AddressSanitizer runtime the C toolchain ships.
# Runtimes older than LLVM 19 allocate thread contexts from the global
# low-level allocator, which is not thread safe, so they corrupt their own
# thread registry when the Go runtime creates and retires threads
# concurrently and abort test binaries at random in arbitrary packages
# (llvm/llvm-project#87324, fixed by llvm/llvm-project#88177). GCC's
# libsanitizer snapshot predates that fix as well, so -asan is only reliable
# when a clang >= 19 runtime is available.
asan_runtime_major() {
  local version
  version=$("${1}" --version 2>/dev/null | head -1) || return 1
  [[ "${version}" = *"clang version "* ]] || return 1
  version=${version##*clang version }
  echo "${version%%.*}"
}

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
      asan_cc=${CC:-}
      if [[ -z "${asan_cc}" ]]; then
        # Prefer the default clang when it is new enough, otherwise the
        # newest explicitly installed one.
        for candidate in clang clang-21 clang-20 clang-19; do
          command -v "${candidate}" >/dev/null 2>&1 || continue
          major=$(asan_runtime_major "${candidate}") || continue
          if [[ "${major}" -ge 19 ]]; then
            asan_cc=${candidate}
            break
          fi
        done
      fi

      major=$(asan_runtime_major "${asan_cc:-false}") || major=0
      if [[ "${major}" -ge 19 ]]; then
        test_args=("-asan")
        if [[ -z "${CC:-}" ]]; then
          export CC=${asan_cc}
          if [[ -z "${CXX:-}" ]] && command -v "${asan_cc/clang/clang++}" >/dev/null 2>&1; then
            export CXX=${asan_cc/clang/clang++}
          fi
        fi
      else
        # Every available ASan runtime predates the fix; -asan would abort at
        # random, so run the race detector instead.
        test_args=("-race")
      fi
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

# parquet/file holds the large-value regression tests, which are what exhaust
# the 7 GB macOS ARM64 runners when they run alongside other packages. Give
# that one package its own invocation there and let the rest of parquet keep
# its default parallelism: serializing every package instead (-p=1) cost the
# macOS jobs about 4.5 minutes each and pushed them into their CI timeout.
parquet_pkgs=("./...")
serial_pkgs=()
if [[ "$(go env GOOS)" = "darwin" ]]; then
  parquet_pkgs=()
  while IFS= read -r pkg; do
    if [[ "${pkg}" = */parquet/file ]]; then
      serial_pkgs+=("${pkg}")
    else
      parquet_pkgs+=("${pkg}")
    fi
  done < <(go list ./...)
fi

for parquet_tags in assert assert,noasm; do
  go test "${test_args[@]}" -tags "${parquet_tags}" "${parquet_pkgs[@]}"
  if [[ ${#serial_pkgs[@]} -gt 0 ]]; then
    go test "${test_args[@]}" -tags "${parquet_tags}" "${serial_pkgs[@]}"
  fi
done

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

# The release verification runs this script over an extracted archive, which is
# not a git checkout. Compare the regenerated files only in a repository.
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git diff --exit-code -- .
fi

popd

# The run above uses the released arrow-go that arrgen's go.mod names, which is
# what a consumer gets. Run it against this tree too: the equivalence tests are
# what catch an arrow/array/arreflect change the generated encoders no longer
# match, and in module mode they would not see one until the next release.
pushd "${source_dir}"

rm -f go.work go.work.sum
go work init . ./arrgen
go test "${test_args[@]}" ./arrgen/...
rm -f go.work go.work.sum

popd

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

# --run and --aggregate split a benchmark run so CI can shard packages across
# parallel jobs and then combine the shards into one JSON for a single upload.
# See usage() for the full interface.

set -exo pipefail

GOBENCHDATA_VERSION="v1.3.1"

usage() {
  cat >&2 <<'EOF'
Usage:
  bench.sh <source_dir> [--json|-json]
      Run every benchmark (BENCH_PACKAGES, default "./..."); with --json/-json
      also aggregate into "bench_stats.json". Removes .dat files afterwards.
  bench.sh <source_dir> --run [--packages "<patterns>"] [--out <file>] [--timeout <dur>]
      Run benchmarks only and write raw output to <file> (default
      "bench_stat.dat"); leaves it in place for later aggregation.
  bench.sh <source_dir> --aggregate [--dat "<glob>"] [--json-out <file>]
      Combine .dat files (default "<source_dir>/bench_*.dat") into <file>
      (default "bench_stats.json") via gobenchdata.

Environment:
  BENCH_PACKAGES  Default packages for full/--run modes (default "./...").
  BENCH_TIMEOUT   Default per-package `go test` timeout (default "40m").
EOF
}

run_benchmarks() {
  local source_dir="$1" packages="$2" out_file="$3" timeout="$4"

  PARQUET_TEST_DATA="${source_dir}/parquet-testing/data"
  export PARQUET_TEST_DATA

  pushd "${source_dir}" >/dev/null
  # -timeout is applied to each package's test binary separately, so the full
  # "./..." wall-clock cost is the sum across packages; --run shards a subset.
  # shellcheck disable=SC2086  # intentional word-splitting of package patterns
  go test -bench=. -benchmem -timeout "${timeout}" -run='^$' ${packages} | tee "${out_file}"
  popd >/dev/null
}

aggregate_results() {
  local dat_glob="$1" json_out="$2"

  go install "go.bobheadxi.dev/gobenchdata@${GOBENCHDATA_VERSION}"
  PATH="$(go env GOPATH)/bin:$PATH"
  export PATH

  # shellcheck disable=SC2086  # dat_glob is an intentional glob / list of files
  cat ${dat_glob} | gobenchdata --json "${json_out}"
}

if [ -z "${1:-}" ]; then
  echo "Error: Missing source directory argument" >&2
  usage
  exit 1
fi

source_dir="$1"
shift

mode="${1:-}"

packages="${BENCH_PACKAGES:-./...}"
timeout="${BENCH_TIMEOUT:-40m}"

case "${mode}" in
"" | -json | --json)
  run_benchmarks "${source_dir}" "${packages}" "bench_stat.dat" "${timeout}"
  if [[ "${mode}" == "-json" || "${mode}" == "--json" ]]; then
    aggregate_results "${source_dir}/bench_*.dat" "bench_stats.json"
  fi
  rm "${source_dir}"/bench_*.dat
  ;;

--run)
  shift
  out_file="bench_stat.dat"
  while [ "$#" -gt 0 ]; do
    case "$1" in
    --packages)
      packages="$2"
      shift 2
      ;;
    --out)
      out_file="$2"
      shift 2
      ;;
    --timeout)
      timeout="$2"
      shift 2
      ;;
    *)
      echo "Error: unknown --run option: $1" >&2
      usage
      exit 1
      ;;
    esac
  done
  run_benchmarks "${source_dir}" "${packages}" "${out_file}" "${timeout}"
  ;;

--aggregate)
  shift
  dat_glob="${source_dir}/bench_*.dat"
  json_out="bench_stats.json"
  while [ "$#" -gt 0 ]; do
    case "$1" in
    --dat)
      dat_glob="$2"
      shift 2
      ;;
    --json-out)
      json_out="$2"
      shift 2
      ;;
    *)
      echo "Error: unknown --aggregate option: $1" >&2
      usage
      exit 1
      ;;
    esac
  done
  aggregate_results "${dat_glob}" "${json_out}"
  ;;

-h | --help)
  usage
  exit 0
  ;;

*)
  echo "Error: unknown mode: ${mode}" >&2
  usage
  exit 1
  ;;
esac

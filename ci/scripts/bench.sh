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
  BENCH_TIMEOUT   Wall-clock limit for the whole `go test` invocation
                  (default "4h", sized for "./..."); "0" disables it. Nonzero
                  values require GNU timeout (timeout/gtimeout) or Python 3.
EOF
}

run_with_python_timeout() {
  local duration="$1"
  shift
  python3 - "${duration}" "$@" <<'PY'
import os
import signal
import subprocess
import sys

duration = sys.argv[1]
if not duration:
    sys.exit("Error: benchmark timeout cannot be empty")
multipliers = {"s": 1, "m": 60, "h": 60 * 60, "d": 24 * 60 * 60}
suffix = duration[-1]
if suffix in multipliers:
    value = duration[:-1]
    multiplier = multipliers[suffix]
else:
    value = duration
    multiplier = 1

try:
    timeout_seconds = float(value) * multiplier
except ValueError:
    sys.exit(f"Error: invalid benchmark timeout: {duration!r}")
if timeout_seconds <= 0:
    sys.exit(f"Error: benchmark timeout must be greater than zero: {duration!r}")

process = subprocess.Popen(sys.argv[2:], start_new_session=True)


def forward_signal(signum, _frame):
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass


for forwarded_signal in (
    signal.SIGHUP,
    signal.SIGINT,
    signal.SIGQUIT,
    signal.SIGTERM,
):
    signal.signal(forwarded_signal, forward_signal)

try:
    returncode = process.wait(timeout=timeout_seconds)
except subprocess.TimeoutExpired:
    forward_signal(signal.SIGQUIT, None)
    try:
        process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    sys.exit(124)

if returncode < 0:
    returncode = 128 - returncode
sys.exit(returncode)
PY
}

run_benchmarks() {
  local source_dir="$1" packages="$2" out_file="$3" timeout="$4"

  PARQUET_TEST_DATA="${source_dir}/parquet-testing/data"
  export PARQUET_TEST_DATA

  # `go test -timeout` does not cover benchmarks: the testing package stops its
  # alarm before running them, so a runaway benchmark would otherwise burn the
  # full 6h GitHub Actions job limit. Bound the whole run with a process
  # watchdog instead, so a shard that goes pathological fails fast and visibly.
  local runner=()
  if [ "${timeout}" != "0" ]; then
    local timeout_command=""
    local candidate
    for candidate in timeout gtimeout; do
      if command -v "${candidate}" >/dev/null 2>&1 &&
        "${candidate}" --signal=QUIT --kill-after=1m 0 true >/dev/null 2>&1; then
        timeout_command="${candidate}"
        break
      fi
    done

    if [ -n "${timeout_command}" ]; then
      runner=("${timeout_command}" --signal=QUIT --kill-after=1m "${timeout}")
    elif command -v python3 >/dev/null 2>&1; then
      runner=(run_with_python_timeout "${timeout}")
    else
      echo "Error: no compatible watchdog is available for benchmark timeout '${timeout}'." >&2
      echo "Install GNU coreutils (for example, 'brew install coreutils' on macOS) or Python 3, or set BENCH_TIMEOUT=0 / pass --timeout 0 to disable the timeout guard." >&2
      return 1
    fi
  fi

  pushd "${source_dir}" >/dev/null
  # shellcheck disable=SC2086  # intentional word-splitting of package patterns
  "${runner[@]}" go test -bench=. -benchmem -run='^$' ${packages} | tee "${out_file}"
  popd >/dev/null
}

aggregate_results() {
  local dat_glob="$1" json_out="$2"
  local dat_files=()
  local dat_file

  go install "go.bobheadxi.dev/gobenchdata@${GOBENCHDATA_VERSION}"
  PATH="$(go env GOPATH)/bin:$PATH"
  export PATH

  while IFS= read -r dat_file; do
    dat_files[${#dat_files[@]}]="${dat_file}"
  done < <(compgen -G "${dat_glob}")
  if [ "${#dat_files[@]}" -eq 0 ]; then
    echo "Error: no benchmark data files match: ${dat_glob}" >&2
    return 1
  fi
  cat -- "${dat_files[@]}" | gobenchdata --json "${json_out}"
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
# Sized for the whole "./..." suite; CI passes a tighter --timeout per shard.
timeout="${BENCH_TIMEOUT:-4h}"

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

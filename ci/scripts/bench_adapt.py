#!/usr/bin/env python3
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

import argparse
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from benchadapt import BenchmarkResult
from benchadapt._machine_info import machine_info as collect_machine_info
from benchadapt.adapters import BenchmarkAdapter
from benchadapt.log import log

log.setLevel(logging.DEBUG)

ARROW_ROOT = Path(__file__).parent.parent.parent.resolve()
SCRIPTS_PATH = ARROW_ROOT / "ci" / "scripts"
DEFAULT_RESULT_FILE = Path("bench_stats.json")
MACHINE_INFO_PATTERN = "*.dat.machine.json"


def upload_context() -> Tuple[Dict[str, object], str]:
    # `github_commit_info` is meant to communicate GitHub-flavored commit
    # information to Conbench. See
    # https://github.com/conbench/conbench/blob/cf7931f/benchadapt/python/benchadapt/result.py#L66
    # for a specification.
    github_commit_info: Dict[str, object] = {
        "repository": "https://github.com/apache/arrow-go"
    }

    if os.environ.get("CONBENCH_REF") == "main":
        # Assume GitHub Actions CI. The environment variable lookups below are
        # expected to fail when not running in GitHub Actions.
        github_commit_info = {
            "repository": (
                f'{os.environ["GITHUB_SERVER_URL"]}/{os.environ["GITHUB_REPOSITORY"]}'
            ),
            "commit": os.environ["GITHUB_SHA"],
            "pr_number": None,  # implying default branch
        }
        run_reason = "commit"
    else:
        # Assume that the environment is not GitHub Actions CI. Error out if
        # that assumption seems to be wrong.
        assert os.getenv("GITHUB_ACTIONS") is None

        # This is probably a local dev environment, for testing. In this case,
        # it does usually not make sense to provide commit information (not a
        # controlled CI environment). Explicitly leave out "commit" and
        # "pr_number" to reflect that (to not send commit information).

        # Reflect 'local dev' scenario in run_reason. Allow user to (optionally)
        # inject a custom piece of information into the run reason here, from
        # environment.
        run_reason = "localdev"
        custom_reason_suffix = os.getenv("CONBENCH_CUSTOM_RUN_REASON")
        if custom_reason_suffix is not None:
            run_reason += f" {custom_reason_suffix.strip()}"

    return github_commit_info, run_reason


def capture_machine_info(dat_file: Path) -> Path:
    packages = []
    seen_packages = set()
    with dat_file.open("r", encoding="utf-8") as source:
        for line in source:
            if not line.startswith("pkg: "):
                continue
            package = line.removeprefix("pkg: ").strip()
            if not package:
                raise ValueError(f"Empty 'pkg: ' header in {dat_file}")
            if package not in seen_packages:
                packages.append(package)
                seen_packages.add(package)

    if not packages:
        raise ValueError(f"No 'pkg: ' headers found in {dat_file}")

    sidecar = Path(f"{dat_file}.machine.json")
    with sidecar.open("w", encoding="utf-8") as sink:
        json.dump(
            {
                "packages": packages,
                "machine_info": collect_machine_info(),
            },
            sink,
            indent=2,
            sort_keys=True,
        )
        sink.write("\n")
    return sidecar


def load_machine_info(directory: Path) -> Dict[str, Dict[str, object]]:
    if not directory.is_dir():
        raise ValueError(f"Machine metadata directory does not exist: {directory}")

    sidecars = sorted(directory.glob(MACHINE_INFO_PATTERN))
    if not sidecars:
        raise ValueError(
            f"No machine metadata sidecars matching {MACHINE_INFO_PATTERN!r} "
            f"found in {directory}"
        )

    machine_info_by_package: Dict[str, Dict[str, object]] = {}
    source_by_package: Dict[str, Path] = {}
    for sidecar in sidecars:
        with sidecar.open("r", encoding="utf-8") as source:
            payload = json.load(source)

        if not isinstance(payload, dict) or set(payload) != {
            "packages",
            "machine_info",
        }:
            raise ValueError(
                f"Invalid machine metadata schema in {sidecar}; expected only "
                "'packages' and 'machine_info'"
            )

        packages = payload["packages"]
        if (
            not isinstance(packages, list)
            or not packages
            or not all(
                isinstance(package, str)
                and package
                and package == package.strip()
                for package in packages
            )
        ):
            raise ValueError(f"Invalid package list in machine metadata {sidecar}")
        if len(packages) != len(set(packages)):
            raise ValueError(f"Duplicate package in machine metadata {sidecar}")

        machine_info = payload["machine_info"]
        if not isinstance(machine_info, dict) or not machine_info:
            raise ValueError(f"Invalid machine_info in machine metadata {sidecar}")

        for package in packages:
            if package in machine_info_by_package:
                raise ValueError(
                    f"Ambiguous machine metadata for package {package!r}: "
                    f"{source_by_package[package]} and {sidecar}"
                )
            machine_info_by_package[package] = machine_info
            source_by_package[package] = sidecar

    return machine_info_by_package


class GoAdapter(BenchmarkAdapter):
    def __init__(
        self,
        *args,
        results_file: Optional[Path] = None,
        machine_info_dir: Optional[Path] = None,
        **kwargs,
    ) -> None:
        reuse_results = results_file is not None
        if reuse_results != (machine_info_dir is not None):
            raise ValueError(
                "Explicit result reuse requires both results_file and machine_info_dir"
            )

        if reuse_results:
            self.result_file = Path(results_file)
            if not self.result_file.is_file():
                raise ValueError(
                    f"Benchmark results file does not exist: {self.result_file}"
                )
            self.machine_info_by_package = load_machine_info(Path(machine_info_dir))
            command = ["true"]
        else:
            # A pre-existing bench_stats.json must never opt the default path
            # into reuse. The benchmark command overwrites it with a fresh run.
            self.result_file = DEFAULT_RESULT_FILE
            self.machine_info_by_package = None
            command = ["bash", SCRIPTS_PATH / "bench.sh", ARROW_ROOT, "-json"]

        self.github_commit_info, self.run_reason = upload_context()
        super().__init__(command=command, *args, **kwargs)

    def _transform_results(self) -> List[BenchmarkResult]:
        with self.result_file.open("r", encoding="utf-8") as source:
            raw_results = json.load(source)

        suites = raw_results[0]["Suites"]
        if self.machine_info_by_package is not None:
            missing_packages = sorted(
                {
                    suite["Pkg"]
                    for suite in suites
                    if suite["Pkg"] not in self.machine_info_by_package
                }
            )
            if missing_packages:
                raise ValueError(
                    "Missing machine metadata for benchmark package(s): "
                    + ", ".join(missing_packages)
                )

        run_id = uuid.uuid4().hex
        parsed_results = []
        for suite in suites:
            batch_id = uuid.uuid4().hex
            package = suite["Pkg"]

            for benchmark in suite["Benchmarks"]:
                data = benchmark["Mem"]["MBPerSec"] * 1e6
                time = 1 / benchmark["NsPerOp"] * 1e9

                name = benchmark["Name"].removeprefix("Benchmark")
                ncpu = name[name.rfind("-") + 1 :]
                pieces = name[: -(len(ncpu) + 1)].split("/")

                result_fields = {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "stats": {
                        "data": [data],
                        "unit": "B/s",
                        "times": [time],
                        "time_unit": "i/s",
                        "iterations": benchmark["Runs"],
                    },
                    "context": {
                        "benchmark_language": "Go",
                        "goos": suite["Goos"],
                        "goarch": suite["Goarch"],
                    },
                    "tags": {
                        "pkg": package,
                        "num_cpu": ncpu,
                        "name": pieces[0],
                        "params": "/".join(pieces[1:]),
                    },
                    "run_reason": self.run_reason,
                    "github": self.github_commit_info,
                }
                if self.machine_info_by_package is not None:
                    result_fields["machine_info"] = self.machine_info_by_package[package]

                parsed = BenchmarkResult(**result_fields)
                parsed.run_name = (
                    f"{parsed.run_reason}: {self.github_commit_info.get('commit')}"
                )
                parsed_results.append(parsed)

        return parsed_results


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--capture-machine-info",
        type=Path,
        metavar="DAT",
        help="write DAT.machine.json using this machine and DAT's package headers",
    )
    mode.add_argument(
        "--results",
        type=Path,
        help="reuse an explicitly supplied gobenchdata JSON result file",
    )
    parser.add_argument(
        "--machine-info-dir",
        type=Path,
        help="directory of per-shard .dat.machine.json provenance sidecars",
    )
    args = parser.parse_args()

    if args.capture_machine_info is not None:
        if args.machine_info_dir is not None:
            parser.error("--machine-info-dir cannot be used with --capture-machine-info")
        capture_machine_info(args.capture_machine_info)
        return

    if (args.results is None) != (args.machine_info_dir is None):
        parser.error("--results and --machine-info-dir must be supplied together")

    go_adapter = GoAdapter(
        results_file=args.results,
        machine_info_dir=args.machine_info_dir,
        result_fields_override={"info": {}},
    )
    go_adapter()


if __name__ == "__main__":
    main()
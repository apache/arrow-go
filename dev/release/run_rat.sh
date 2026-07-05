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

set -eu

RELEASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RAT_VERSION=0.16.1

RAT_JAR="${RELEASE_DIR}/apache-rat-${RAT_VERSION}.jar"
RAT_SHA_FILE="${RELEASE_DIR}/apache-rat-${RAT_VERSION}.jar.sha512"
if [ ! -f "${RAT_JAR}" ]; then
  curl \
    --fail \
    --output "${RAT_JAR}" \
    --show-error \
    --silent \
    https://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar
fi

if [ ! -f "${RAT_SHA_FILE}" ]; then
  echo "Missing RAT checksum file: ${RAT_SHA_FILE}" >&2
  exit 1
fi

if ! ${PYTHON:-python3} - <<'PY' "${RAT_JAR}" "${RAT_SHA_FILE}"
import hashlib
import pathlib
import sys

actual = hashlib.sha512(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest()
expected = pathlib.Path(sys.argv[2]).read_text().strip().split()[0]
if actual != expected:
    print(f"checksum mismatch for RAT jar {sys.argv[1]}")
    print(f"expected: {expected}")
    print(f"actual:   {actual}")
    raise SystemExit(1)
PY
then
  echo "checksum verification failed for ${RAT_JAR}" >&2
  exit 1
fi

RAT_XML="${RELEASE_DIR}/rat.xml"
java \
  -jar "${RAT_JAR}" \
  --out "${RAT_XML}" \
  --xml \
  "$1"
FILTERED_RAT_TXT="${RELEASE_DIR}/filtered_rat.txt"
if ${PYTHON:-python3} \
  "${RELEASE_DIR}/check_rat_report.py" \
  "${RELEASE_DIR}/rat_exclude_files.txt" \
  "${RAT_XML}" > \
  "${FILTERED_RAT_TXT}"; then
  echo "No unapproved licenses"
else
  cat "${FILTERED_RAT_TXT}"
  N_UNAPPROVED=$(grep -c "NOT APPROVED" "${FILTERED_RAT_TXT}")
  echo "${N_UNAPPROVED} unapproved licenses. Check Rat report: ${RAT_XML}"
  exit 1
fi

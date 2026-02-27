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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <rc>"
  echo " e.g.: $0 1"
  exit 1
fi

rc=$1

: "${RELEASE_DEFAULT:=1}"
: "${RELEASE_PULL:=${RELEASE_DEFAULT}}"
: "${RELEASE_PUSH_TAG:=${RELEASE_DEFAULT}}"
: "${RELEASE_SIGN:=${RELEASE_DEFAULT}}"
: "${RELEASE_UPLOAD:=${RELEASE_DEFAULT}}"

cd "${SOURCE_TOP_DIR}"

if [ "${RELEASE_PULL}" -gt 0 ] || [ "${RELEASE_PUSH_TAG}" -gt 0 ]; then
  git_origin_url="$(git remote get-url origin)"
  if [ "${git_origin_url}" != "git@github.com:apache/arrow-go.git" ]; then
    echo "This script must be ran with working copy of apache/arrow-go."
    echo "The origin's URL: ${git_origin_url}"
    exit 1
  fi
fi

if [ "${RELEASE_PULL}" -gt 0 ]; then
  echo "Ensure using the latest commit"
  git checkout main
  git pull --rebase --prune
fi

version=$(grep -o '^const PkgVersion = ".*"' "arrow/doc.go" |
  sed \
    -e 's/^const PkgVersion = "//' \
    -e 's/"$//')

rc_tag="v${version}-rc${rc}"
if [ "${RELEASE_PUSH_TAG}" -gt 0 ]; then
  echo "Tagging for RC: ${rc_tag}"
  git tag -a -m "${version} RC${rc}" "${rc_tag}"
  git push origin "${rc_tag}"
fi

rc_hash="$(git rev-list --max-count=1 "${rc_tag}")"

id="apache-arrow-go-${version}"
tar_gz="${id}.tar.gz"

git_origin_url="$(git remote get-url origin)"
repository="${git_origin_url#*github.com?}"
repository="${repository%.git}"

if [ "${RELEASE_SIGN}" -gt 0 ]; then
  echo "Looking for GitHub Actions workflow on ${repository}:${rc_tag}"
  run_id=""
  while [ -z "${run_id}" ]; do
    echo "Waiting for run to start..."
    run_id=$(gh run list \
      --repo "${repository}" \
      --workflow=rc.yml \
      --json 'databaseId,event,headBranch,status' \
      --jq ".[] | select(.event == \"push\" and .headBranch == \"${rc_tag}\") | .databaseId")
    sleep 1
  done

  echo "Found GitHub Actions workflow with ID: ${run_id}"
  gh run watch --repo "${repository}" --exit-status "${run_id}"

  echo "Downloading .tar.gz from GitHub Releases"
  gh release download "${rc_tag}" \
    --dir . \
    --pattern "${tar_gz}" \
    --repo "${repository}" \
    --skip-existing

  echo "Signing tar.gz and creating checksums"
  gpg --armor --output "${tar_gz}.asc" --detach-sig "${tar_gz}"
fi

if [ "${RELEASE_UPLOAD}" -gt 0 ]; then
  echo "Uploading signature"
  gh release upload "${rc_tag}" \
    --clobber \
    --repo "${repository}" \
    "${tar_gz}.asc"
fi

gh release download "${rc_tag}" \
  --dir . \
  --pattern "${tar_gz}.sha512" \
  --repo "${repository}" \
  --clobber

SOURCE_TARBALL_HASH=$(awk '{print $1}' "${tar_gz}.sha512")
rm -f "${tar_gz}.sha512"

echo "Draft email for dev@arrow.apache.org mailing list"
echo ""
echo "---------------------------------------------------------"
cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE][Go] Release Apache Arrow Go ${version} RC${rc}

Hi,

I would like to propose the following release candidate (RC${rc}) of
Apache Arrow Go version ${version}.

This release candidate is based on commit:
${rc_hash} [1]

The source release rc${rc} is hosted at [2].

This is not a permanent URL. If the RC is accepted, it will be moved to 
the final release location. The SHA512 hash of the source tarball is:
${SOURCE_TARBALL_HASH}.

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [3] for how to validate a release candidate.

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow Go ${version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow Go ${version} because...

[1]: https://github.com/apache/arrow-go/tree/${rc_hash}
[2]: https://github.com/apache/arrow-go/releases/${rc_tag}
[3]: https://github.com/apache/arrow-go/blob/main/dev/release/README.md#verify
MAIL
echo "---------------------------------------------------------"

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

set -e
set -u

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROW_DIR="${SOURCE_DIR}/../../"
: ${ARROW_SITE_DIR:="${ARROW_DIR}/../arrow-site"}

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <previous-version> <version>"
  exit 1
fi

previous_version=$1
version=$2

branch_name=release-note-arrow-go-${version}
release_dir="${ARROW_SITE_DIR}/_posts"
announce_file="${release_dir}/$(date +%Y-%m-%d)-arrow-go-${version}.md"

pushd "${ARROW_SITE_DIR}"
git_origin_url="$(git remote get-url origin)"
if [ "${git_origin_url}" = "git@github.com:apache/arrow-site.git" ]; then
  echo "This script should be run with a fork of apache/arrow-site."
  exit 1
fi

DEFAULT_BRANCH="$(git rev-parse --abbrev-ref origin/HEAD | sed s@origin/@@)"
git fetch --all --prune --tags --force -j$(nproc)
git switch ${DEFAULT_BRANCH}
git branch -D ${branch_name} || :
git switch -c ${branch_name}
popd

pushd "${ARROW_DIR}"

previous_major_version="$(echo ${previous_version} | cut -d. -f1)"
previous_minor_version="$(echo ${previous_version} | cut -d. -f2)"
major_version="$(echo ${version} | cut -d. -f1)"
minor_version="$(echo ${version} | cut -d. -f2)"
if [ ${previous_major_version} -eq ${major_version} ]; then
  if [ ${previous_minor_version} -eq ${minor_version} ]; then
    release_type=patch
  else
    release_type=minor
  fi
else
  release_type=major
fi

export TZ=UTC
release_date=$(LC_TIME=C date "+%-d %B %Y")
release_date_iso8601=$(LC_TIME=C date "+%Y-%m-%d")
previous_tag_date=$(git log -n 1 --pretty=%aI v${previous_version})
rough_previous_release_date=$(date --date "${previous_tag_date}" +%s)
rough_release_date=$(date +%s)
rough_n_development_months=$(((\
  ${rough_release_date} - ${rough_previous_release_date}) / (60 * 60 * 24 * 30)))

git_tag=v${version}
git_range=v${previous_version}..v${version}

contributors_command_line="git shortlog -sn ${git_range}"
contributors=$(${contributors_command_line} | grep -v dependabot)

n_commits=$(git log --pretty=oneline ${git_range} | grep -i -v "chore: Bump" | wc -l)
n_contributors=$(${contributors_command_line} | grep -v dependabot | wc -l)

git_tag_hash=$(git log -n 1 --pretty=%H ${git_tag})
git_changelog="$(gh release view --json body --jq .body | grep -v '@dependabot' | sed -e 's/^#/##/g')"
popd

pushd "${ARROW_SITE_DIR}"

cat <<ANNOUNCE >>"${announce_file}"
---
layout: post
title: "Apache Arrow Go ${version} Release"
date: "${release_date_iso8601} 00:00:00"
author: pmc
categories: [release]
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

The Apache Arrow team is pleased to announce the v${version} release of Apache Arrow Go. 
This ${release_type} release covers ${n_commits} commits from ${n_contributors} distinct contributors.

## Contributors
\`\`\`console
$ ${contributors_command_line}
ANNOUNCE

echo "${contributors}" >>"${announce_file}"

cat <<ANNOUNCE >>"${announce_file}"
\`\`\`

## Changelog

${git_changelog}
ANNOUNCE

git add "${announce_file}"
git commit -m "[Release] Add release notes for Arrow Go ${version}"
git push -u origin ${branch_name}

github_url=$(git remote get-url origin |
  sed \
    -e 's,^git@github.com:,https://github.com/,' \
    -e 's,\.git$,,')

echo "Success!"
echo "Create a pull request:"
echo "  ${github_url}/pull/new/${branch_name}"
popd

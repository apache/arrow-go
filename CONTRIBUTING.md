<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# How to contribute to Apache Arrow Go

We utilize [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for our commit messages. This helps maintain the semantic 
versioning of this module. 

Please use the following commit types: `build`, `chore`, `ci`, `docs`, 
`feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`.

For PRs with changes entirely within a single sub-package, please use
a scope that references that package such as `arrow/flight` or 
`parquet/pqarrow`. For more general PRs, a top level scope should be
sufficient.

For example:

```
fix(arrow/cdata): handle empty structs in C data interface

ci: update CI environment

feat(parquet): support new encoding type
```

## Did you find a bug?

The Arrow project uses GitHub as a bug tracker.  To report a bug, sign in 
to your GitHub account, navigate to [GitHub issues](https://github.com/apache/arrow-go/issues) and click on **New issue** .

To be assigned to an issue, add a comment "take" to that issue.

Before you create a new bug entry, we recommend you first search among 
existing Arrow issues in [GitHub](https://github.com/apache/arrow-go/issues).

## Did you write a patch that fixes a bug or brings an improvement?

If there is a corresponding issue for your patch, please make sure to 
[reference the issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword) in your PR description.

## Do you want to propose a significant new feature or an important refactoring?

We ask that all discussions about major changes in the codebase happen
publicly on the [arrow-dev mailing-list](https://arrow.apache.org/community/#mailing-lists).

## Do you have questions about the source code, the build procedure or the development process?

You can also ask on the mailing-list, see above.

## Further information

Please read our [development documentation](https://arrow.apache.org/docs/developers/index.html)
or look through the [New Contributor's Guide](https://arrow.apache.org/docs/developers/guide/index.html).

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package integration holds setup shared by the programs used for
// cross-implementation integration testing (the IPC JSON integration and
// file/stream conversion programs and the Flight integration client and
// server). Importing it registers the canonical extension types, so that
// those programs read and write them, including as dictionary value types,
// like other implementations do.
//
// The C Data integration library does not import it: the C Data importer
// does not consult the extension type registry.
package integration

import (
	// register the canonical extension types
	_ "github.com/apache/arrow-go/v18/arrow/extensions"
)

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

// Package encryption provides optional KMS integration for reading parquet
// files encrypted with PKMT1 key material, the format written by the
// CryptoFactory of parquet-cpp and pyarrow (both single and double key
// wrapping).
//
// It plugs into the existing low level API as a
// parquet.DecryptionKeyRetriever: pass a KmsClient implementation to
// NewKmsKeyRetriever and set the retriever with
// parquet.WithKeyRetriever. Files using explicit keys or custom key
// metadata continue to work without this package.
//
// This package is optional: it carries no effect on code that does not
// import it.
package encryption

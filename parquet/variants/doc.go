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

// This package contains utilities to marshal and unmarshal data to and from the Variant
// encoding format as described in
// [the Variant encoding spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
//
// There are two main ways to create a marshaled Variant:
//
//  1. Using `variants.Marshal()`. Simply pass in the value you'd like to marshal, and all the
//     type inference is done for you. Structs and string-keyed maps will be converted into
// 	objects, slices will be converted into arrays, and primitives will be encoded with the
// 	appropriate primitive type. This will feel like the JSON library's `Marshal()`.
//  2. Using `variants.NewBuilder()`. This allows you to build out your Variant bit by bit.
//
// To convert from a marshaled Variant back to a type, use `variants.Unmarshal()`. Like the JSON
// `Unmarshal()`, this takes in a pointer to a value to "fill up." Objects can be unmarshaled into
// either structs or string-keyed maps, arrays can be unmarshaled into slices, and primitives into
// primitives.
//
// This library does have a few shortcomings, namely in that the Metadata is always marshaled with
// unordered keys (done to make marshaling considerably easier to code up), and that currently,
// unmarshaling decodes the whole Variant, not just a specific field.

package variants

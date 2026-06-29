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

// EXPERIMENTAL: VECTOR logical type + repetition.
//
// This is a *fragment*, not a complete IDL. arrow-go generates
// parquet/internal/gen-go/parquet/parquet.go from a full upstream
// parquet.thrift supplied externally at generation time (see the
// `//go:generate thrift -o internal -r --gen go ../parquet.thrift` directive
// in parquet/doc.go); that complete file is not vendored here.
//
// The VECTOR additions below are not yet part of upstream apache/parquet-format.
// Until they are, the corresponding additions in the generated parquet.go were
// applied by hand, matching the Thrift 0.21.0 code generator that produced the
// rest of that file. Byte-identical regeneration therefore requires:
//   1. merging the snippets below into the upstream parquet.thrift, and
//   2. running the Thrift 0.21.0 compiler (newer compilers reformat the file).
//
// This fragment is the source of truth for the IDL; keep it in sync with the
// hand-applied changes in parquet/internal/gen-go/parquet/parquet.go.
//
// Reference: rok/arrow#51 (Parquet C++ prototype) and the
// "Fixed-size list type for Parquet" design document.

// ---------------------------------------------------------------------------
// enum FieldRepetitionType  (add the VECTOR member)
// ---------------------------------------------------------------------------

enum FieldRepetitionType {
  /** The field is required (can not be null) and each parent value MUST have
   * exactly one value. */
  REQUIRED = 0;

  /** The field is optional (can be null) and each parent value can have 0 or
   * 1 values. */
  OPTIONAL = 1;

  /** The field is repeated and can contain 0 or more values */
  REPEATED = 2;

  // EXPERIMENTAL: fixed-size repetition.
  //
  // VECTOR repeats exactly vector_length times for every parent value and,
  // unlike REPEATED, does not increase the maximum definition or repetition
  // level of descendants. It is used only on the middle group of the canonical
  // VECTOR layout:
  //
  //   required group embedding (VECTOR) {
  //     vector group list [vector_length] {
  //       required float element;
  //     }
  //   }
  //
  // The VECTOR-repeated group carries vector_length, has exactly one element
  // child, and is the single child of an outer group annotated with
  // LogicalType.VECTOR. Writers emit vector_length element slots contiguously
  // per vector and do not split one vector across data pages.
  VECTOR = 3;
}

// ---------------------------------------------------------------------------
// struct VectorType + LogicalType union member
// ---------------------------------------------------------------------------

// EXPERIMENTAL: annotation for the outer group of a fixed-size vector field.
struct VectorType {}

union LogicalType {
  // ... existing fields 1..18 ...

  // EXPERIMENTAL: fixed-size vector logical type.
  19: VectorType VECTOR;
}

// ---------------------------------------------------------------------------
// struct SchemaElement  (add vector_length, field id 11)
// ---------------------------------------------------------------------------

struct SchemaElement {
  // ... existing fields 1..10 ...

  // vector_length MUST be set, and positive, when repetition_type is VECTOR
  // and MUST NOT be set otherwise. Zero-length vectors are not represented as
  // VECTOR and use the standard LIST encoding.
  11: optional i32 vector_length;
}

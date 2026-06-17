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

// EXPERIMENTAL: VECTOR repetition — reduced Option B.
//
// This is a *fragment*, not a complete IDL. arrow-go generates
// parquet/internal/gen-go/parquet/parquet.go from a full upstream
// parquet.thrift that is supplied externally at generation time (see the
// `//go:generate thrift -o internal -r --gen go ../parquet.thrift` directive
// in parquet/doc.go); that complete file is not vendored in this repository.
//
// The VECTOR additions below are not yet part of upstream apache/parquet-format.
// Until they are, the corresponding additions in the generated parquet.go were
// applied by hand, matching the Thrift 0.21.0 code generator that produced the
// rest of that file. Byte-identical regeneration therefore requires:
//   1. merging the snippets below into the upstream parquet.thrift, and
//   2. running the Thrift 0.21.0 compiler (the version that produced the
//      current parquet.go; newer compilers reformat the entire file).
//
// This fragment is the source of truth for the IDL; keep it in sync with the
// hand-applied changes in parquet/internal/gen-go/parquet/parquet.go.
//
// Reference: rok/arrow#51 (Parquet C++ Option B prototype) and the
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

  // EXPERIMENTAL VECTOR layout rules (kept out of the doc comment so they are
  // not copied into generated code):
  //
  // A VECTOR field repeats exactly vector_length times for every parent value
  // and, unlike REPEATED, does not increase the maximum definition or
  // repetition level of its descendants: readers reconstruct vector values
  // from the fixed multiplicity declared in the schema instead of decoding
  // repetition levels.
  //
  // This reduced Option B implementation only allows VECTOR on primitive leaf
  // fields:
  //
  //   vector <element-type> <name> [vector_length];
  //
  // - The VECTOR leaf carries vector_length and MUST be a primitive field.
  // - The vector value and its element values are non-nullable in this reduced
  //   implementation. Nullable Arrow FixedSizeList values or nullable elements
  //   are represented with the standard LIST encoding instead.
  // - For each parent record, writers MUST emit exactly vector_length physical
  //   leaf values contiguously. Column chunk num_values is therefore
  //   parent_row_count * vector_length.
  // - Writers MUST NOT split the slots of one vector value across data pages;
  //   pages begin and end on whole-vector boundaries.
  // - Statistics are computed over flattened element values, not over
  //   lexicographically ordered vector values.
  // - vector_length MUST be positive: zero-length fixed-size lists are not
  //   represented as VECTOR and use the LIST encoding instead.
  //
  // Readers that do not understand VECTOR are expected to reject the file.

  // EXPERIMENTAL: fixed-size repetition.
  VECTOR = 3;
}

// The reduced leaf-only layout does not add a LogicalType union member: vectors
// are identified solely by FieldRepetitionType.VECTOR plus vector_length on the
// primitive leaf.

// ---------------------------------------------------------------------------
// struct SchemaElement  (add vector_length, field id 11)
// ---------------------------------------------------------------------------

struct SchemaElement {
  // ... existing fields 1..10 ...

  // Field id 11 matches the apache/parquet-format Option B draft
  // (pitrou:vector-repetition) so VECTOR files interoperate with a
  // draft-conformant reader.
  //
  // vector_length MUST be set, and positive, when repetition_type is VECTOR
  // and MUST NOT be set otherwise.  Zero-length vectors are not representable
  // as VECTOR and use the LIST encoding (see FieldRepetitionType.VECTOR).

  // EXPERIMENTAL: The fixed number of times the field repeats per parent
  // value when repetition_type is VECTOR.
  11: optional i32 vector_length;
}

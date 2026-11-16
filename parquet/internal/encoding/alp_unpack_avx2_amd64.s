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

//go:build !noasm

#include "textflag.h"

// The kernels decode groups of 8 deltas of w bits, 1 to 25. A group spans w
// bytes, so each starts on a byte boundary. Its first 4 deltas lie in the 16
// bytes from its start, and its last 4 in the 16 bytes from w/2 bytes in.
// Both loads go in one register, a shuffle moves the 4 bytes holding each
// delta into its own 32-bit lane, and a shift and a mask leave the delta.
//
// The table holds, in order, the shuffle, the shifts and the masks for w.
//
// The values are scaled with two separate multiplications, as Go does, so
// they round the same.

// 3*2^67 and 3*2^67 + 2^52: converting the upper 48 bits of an int64.
DATA alpMagicHigh<>+0(SB)/8, $0x4438000000000000
GLOBL alpMagicHigh<>(SB), RODATA|NOPTR, $8
DATA alpMagicHighSub<>+0(SB)/8, $0x4438001000000000
GLOBL alpMagicHighSub<>(SB), RODATA|NOPTR, $8
// 2^52: converting the lower 16 bits.
DATA alpMagicLow<>+0(SB)/8, $0x4330000000000000
GLOBL alpMagicLow<>(SB), RODATA|NOPTR, $8
// 2^52 + 2^51: converting an integer in [-2^51, 2^51).
DATA alpMagicSmall<>+0(SB)/8, $0x4338000000000000
GLOBL alpMagicSmall<>(SB), RODATA|NOPTR, $8

// UNPACK_GROUP leaves the 8 deltas of the group at SI in Y0, as 32-bit lanes.
#define UNPACK_GROUP \
	VMOVDQU     (SI), X0;                \
	VINSERTI128 $1, (SI)(R8*1), Y0, Y0;  \
	VPSHUFB     Y8, Y0, Y0;              \
	VPSRLVD     Y9, Y0, Y0;              \
	VPAND       Y10, Y0, Y0

// func _alpDecodeGroups32AVX2(src, out unsafe.Pointer, groups, bitWidth int, table *alpAVX2Table, ref uint32, fac, exp float32)
TEXT ·_alpDecodeGroups32AVX2(SB), NOSPLIT, $0-52
	MOVQ src+0(FP), SI
	MOVQ out+8(FP), DI
	MOVQ groups+16(FP), CX
	MOVQ bitWidth+24(FP), DX
	MOVQ table+32(FP), AX
	MOVQ DX, R8
	SHRQ $1, R8
	VMOVDQU      0(AX), Y8
	VMOVDQU      32(AX), Y9
	VMOVDQU      64(AX), Y10
	MOVL         ref+40(FP), BX
	VMOVD        BX, X11
	VPBROADCASTD X11, Y11
	VBROADCASTSS fac+44(FP), Y12
	VBROADCASTSS exp+48(FP), Y13
	TESTQ CX, CX
	JZ    done32

loop32:
	UNPACK_GROUP
	// The frame of reference is added in 32 bits, wrapping as it was
	// subtracted.
	VPADDD    Y11, Y0, Y0
	VCVTDQ2PS Y0, Y0
	VMULPS    Y12, Y0, Y0
	VMULPS    Y13, Y0, Y0
	VMOVUPS   Y0, (DI)
	ADDQ      DX, SI
	ADDQ      $32, DI
	DECQ      CX
	JNZ       loop32

done32:
	VZEROUPPER
	RET

// WIDEN_GROUP widens the deltas in Y0 to 64 bits and adds the frame of
// reference in Y11, wrapping: the first 4 in Y1, the last 4 in Y2.
#define WIDEN_GROUP \
	VPMOVZXDQ    X0, Y1;      \
	VEXTRACTI128 $1, Y0, X2;  \
	VPMOVZXDQ    X2, Y2;      \
	VPADDQ       Y11, Y1, Y1; \
	VPADDQ       Y11, Y2, Y2

// INT64_TO_DOUBLE converts the int64s in src to the nearest doubles, as
// CVTSI2SDQ does, using tmp. The upper 48 bits and the lower 16 each convert
// exactly, so the one rounding is in the final addition.
#define INT64_TO_DOUBLE(src, tmp) \
	VPSRAD   $16, src, tmp;        \
	VPBLENDW $0x33, Y14, tmp, tmp; \
	VPADDQ   Y15, tmp, tmp;        \
	VPBLENDW $0x88, Y4, src, src;  \
	VSUBPD   Y5, tmp, tmp;         \
	VADDPD   tmp, src, src

// func _alpDecodeGroups64AVX2(src, out unsafe.Pointer, groups, bitWidth int, table *alpAVX2Table, ref uint64, fac, exp float64)
TEXT ·_alpDecodeGroups64AVX2(SB), NOSPLIT, $0-64
	MOVQ src+0(FP), SI
	MOVQ out+8(FP), DI
	MOVQ groups+16(FP), CX
	MOVQ bitWidth+24(FP), DX
	MOVQ table+32(FP), AX
	MOVQ DX, R8
	SHRQ $1, R8
	VMOVDQU      0(AX), Y8
	VMOVDQU      32(AX), Y9
	VMOVDQU      64(AX), Y10
	VPBROADCASTQ ref+40(FP), Y11
	VBROADCASTSD fac+48(FP), Y12
	VBROADCASTSD exp+56(FP), Y13
	VPXOR        Y14, Y14, Y14
	VPBROADCASTQ alpMagicHigh<>(SB), Y15
	VPBROADCASTQ alpMagicLow<>(SB), Y4
	VBROADCASTSD alpMagicHighSub<>(SB), Y5
	TESTQ CX, CX
	JZ    done64

loop64:
	UNPACK_GROUP
	WIDEN_GROUP
	INT64_TO_DOUBLE(Y1, Y3)
	INT64_TO_DOUBLE(Y2, Y6)
	VMULPD  Y12, Y1, Y1
	VMULPD  Y12, Y2, Y2
	VMULPD  Y13, Y1, Y1
	VMULPD  Y13, Y2, Y2
	VMOVUPD Y1, (DI)
	VMOVUPD Y2, 32(DI)
	ADDQ    DX, SI
	ADDQ    $64, DI
	DECQ    CX
	JNZ     loop64

done64:
	VZEROUPPER
	RET

// func _alpDecodeGroups64SmallAVX2(src, out unsafe.Pointer, groups, bitWidth int, table *alpAVX2Table, ref uint64, fac, exp float64)
//
// For a vector whose integers all lie in [-2^51, 2^51): adding 2^52 + 2^51
// puts each in the mantissa of a double, and subtracting it as a double
// leaves the integer's value.
TEXT ·_alpDecodeGroups64SmallAVX2(SB), NOSPLIT, $0-64
	MOVQ src+0(FP), SI
	MOVQ out+8(FP), DI
	MOVQ groups+16(FP), CX
	MOVQ bitWidth+24(FP), DX
	MOVQ table+32(FP), AX
	MOVQ DX, R8
	SHRQ $1, R8
	VMOVDQU      0(AX), Y8
	VMOVDQU      32(AX), Y9
	VMOVDQU      64(AX), Y10
	VPBROADCASTQ ref+40(FP), Y11
	VBROADCASTSD fac+48(FP), Y12
	VBROADCASTSD exp+56(FP), Y13
	VPBROADCASTQ alpMagicSmall<>(SB), Y14
	TESTQ CX, CX
	JZ    doneSmall

loopSmall:
	UNPACK_GROUP
	WIDEN_GROUP
	VPADDQ  Y14, Y1, Y1
	VPADDQ  Y14, Y2, Y2
	VSUBPD  Y14, Y1, Y1
	VSUBPD  Y14, Y2, Y2
	VMULPD  Y12, Y1, Y1
	VMULPD  Y12, Y2, Y2
	VMULPD  Y13, Y1, Y1
	VMULPD  Y13, Y2, Y2
	VMOVUPD Y1, (DI)
	VMOVUPD Y2, 32(DI)
	ADDQ    DX, SI
	ADDQ    $64, DI
	DECQ    CX
	JNZ     loopSmall

doneSmall:
	VZEROUPPER
	RET

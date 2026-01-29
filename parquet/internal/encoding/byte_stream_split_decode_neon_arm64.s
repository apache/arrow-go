//+build !noasm !appengine
// NEON implementation following AVX2 algorithm structure with 128-bit vectors

#include "textflag.h"

// func _decodeByteStreamSplitWidth4NEON(data, out unsafe.Pointer, nValues, stride int)
//
// NEON implementation following the AVX2 algorithm structure:
// - Processes suffix FIRST, then vectorized blocks
// - Uses 128-bit NEON vectors (16 bytes per register)
// - Processes 16 float32 values per block (64 bytes total)
// - Uses 2-stage byte unpacking hierarchy
TEXT ·_decodeByteStreamSplitWidth4NEON(SB), NOSPLIT, $0-32
	MOVD data+0(FP), R0      // R0 = data pointer
	MOVD out+8(FP), R1       // R1 = out pointer
	MOVD nValues+16(FP), R2  // R2 = nValues
	MOVD stride+24(FP), R3   // R3 = stride

	// Setup stream pointers
	MOVD R0, R4              // R4 = stream 0
	ADD R3, R0, R5           // R5 = stream 1 = data + stride
	ADD R3, R5, R6           // R6 = stream 2 = data + 2*stride
	ADD R3, R6, R7           // R7 = stream 3 = data + 3*stride

	// Calculate num_blocks = nValues / 16
	LSR $4, R2, R8           // R8 = num_blocks (divide by 16)

	// Calculate num_processed_elements = num_blocks * 16
	LSL $4, R8, R9           // R9 = num_processed_elements

	// First handle suffix (elements beyond complete blocks)
	MOVD R9, R10             // R10 = i = num_processed_elements
	B suffix_check_neon

suffix_loop_neon:
	// Gather bytes: gathered_byte_data[b] = data[b * stride + i]
	MOVBU (R4)(R10), R11      // byte from stream 0
	MOVBU (R5)(R10), R12      // byte from stream 1

	// Calculate output offset: i * 4
	LSL $2, R10, R13         // R13 = i * 4
	ADD R1, R13, R14         // R14 = out + (i * 4)

	// Store gathered bytes
	MOVB R11, (R14)
	MOVB R12, 1(R14)

	MOVBU (R6)(R10), R11      // byte from stream 2
	MOVBU (R7)(R10), R12      // byte from stream 3

	MOVB R11, 2(R14)
	MOVB R12, 3(R14)

	ADD $1, R10, R10

suffix_check_neon:
	CMP R2, R10
	BLT suffix_loop_neon

	// Check if we have blocks to process
	CBZ R9, done_neon        // if num_processed_elements == 0, done

	// Process blocks with NEON
	MOVD $0, R10             // R10 = block index i = 0
	LSR $4, R9, R9           // R9 = num_blocks

block_loop_neon:
	// Calculate offset for this block: i * 16
	LSL $4, R10, R11         // R11 = i * 16

	// Load 16 bytes from each stream - REVERSED for little-endian!
	ADD R7, R11, R12
	VLD1 (R12), [V0.B16]     // V0 = stream 3 (MSB in little-endian)

	ADD R6, R11, R12
	VLD1 (R12), [V1.B16]     // V1 = stream 2

	ADD R5, R11, R12
	VLD1 (R12), [V2.B16]     // V2 = stream 1

	ADD R4, R11, R12
	VLD1 (R12), [V3.B16]     // V3 = stream 0 (LSB in little-endian)

	// Stage 1: Interleave like AVX2 VPUNPCKLBW/VPUNPCKHBW
	VZIP1 V0.B16, V2.B16, V4.B16    // Interleave streams 0,2 low bytes
	VZIP2 V0.B16, V2.B16, V5.B16    // Interleave streams 0,2 high bytes
	VZIP1 V1.B16, V3.B16, V6.B16    // Interleave streams 1,3 low bytes
	VZIP2 V1.B16, V3.B16, V7.B16    // Interleave streams 1,3 high bytes

	// Stage 2: Second level
	VZIP1 V4.B16, V6.B16, V0.B16
	VZIP2 V4.B16, V6.B16, V1.B16
	VZIP1 V5.B16, V7.B16, V2.B16
	VZIP2 V5.B16, V7.B16, V3.B16

	// Store results in sequential order
	// Calculate output base: i * 64
	LSL $6, R10, R11         // R11 = i * 64

	ADD R1, R11, R12
	VST1 [V0.B16], (R12)          // Store at offset 0

	ADD $16, R12
	VST1 [V1.B16], (R12)          // Store at offset 16

	ADD $16, R12
	VST1 [V2.B16], (R12)          // Store at offset 32

	ADD $16, R12
	VST1 [V3.B16], (R12)          // Store at offset 48

	ADD $1, R10, R10
	CMP R9, R10
	BLT block_loop_neon

done_neon:
	RET

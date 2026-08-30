	.build_version macos, 26, 0	sdk_version 26, 5
	.section	__TEXT,__text,regular,pure_instructions
	.globl	_multiply_constant_int32_int32_neon ; -- Begin function multiply_constant_int32_int32_neon
	.p2align	2
_multiply_constant_int32_int32_neon:    ; @multiply_constant_int32_int32_neon
; %bb.0:
	cmp	w2, #4
	b.ge	LBB0_2
; %bb.1:
	mov	w8, #0                          ; =0x0
	cmp	w8, w2
	b.lt	LBB0_5
	b	LBB0_7
LBB0_2:
	fmov	s0, w3
	mov	w8, #4                          ; =0x4
	mov	x9, x0
	mov	x10, x1
	mov	w11, w2
LBB0_3:                                 ; =>This Inner Loop Header: Depth=1
	ldr	q1, [x9], #16
	mul.4s	v1, v1, v0[0]
	str	q1, [x10], #16
	add	x8, x8, #4
	cmp	x8, x11
	b.ls	LBB0_3
; %bb.4:
	and	w8, w2, #0x7ffffffc
	cmp	w8, w2
	b.ge	LBB0_7
LBB0_5:
	mov	w8, w8
LBB0_6:                                 ; =>This Inner Loop Header: Depth=1
	ldr	w9, [x0, x8, lsl #2]
	mul	w9, w9, w3
	str	w9, [x1, x8, lsl #2]
	add	x8, x8, #1
	cmp	w2, w8
	b.gt	LBB0_6
LBB0_7:
	ret
                                        ; -- End function
	.globl	_multiply_constant_int32_int64_neon ; -- Begin function multiply_constant_int32_int64_neon
	.p2align	2
_multiply_constant_int32_int64_neon:    ; @multiply_constant_int32_int64_neon
; %bb.0:
	cmp	w2, #4
	b.ge	LBB1_2
; %bb.1:
	mov	w8, #0                          ; =0x0
	cmp	w8, w2
	b.lt	LBB1_5
	b	LBB1_7
LBB1_2:
	fmov	s0, w3
	lsr	x8, x3, #32
	fmov	s1, w8
	mov	w8, w2
	add	x9, x1, #16
	mov	w10, #4                         ; =0x4
	mov	x11, x0
LBB1_3:                                 ; =>This Inner Loop Header: Depth=1
	ldr	q2, [x11], #16
	sshll.2d	v3, v2, #0
	sshll2.2d	v4, v2, #0
	ext.16b	v5, v3, v3, #8
	zip2.2s	v3, v3, v5
	umull.2d	v3, v3, v0[0]
	umlal.2d	v3, v2, v1[0]
	shl.2d	v3, v3, #32
	umlal.2d	v3, v2, v0[0]
	ext.16b	v5, v4, v4, #8
	zip2.2s	v4, v4, v5
	umull.2d	v4, v4, v0[0]
	umlal2.2d	v4, v2, v1[0]
	shl.2d	v4, v4, #32
	umlal2.2d	v4, v2, v0[0]
	stp	q3, q4, [x9, #-16]
	add	x10, x10, #4
	add	x9, x9, #32
	cmp	x10, x8
	b.ls	LBB1_3
; %bb.4:
	and	w8, w2, #0x7ffffffc
	cmp	w8, w2
	b.ge	LBB1_7
LBB1_5:
	mov	w8, w8
LBB1_6:                                 ; =>This Inner Loop Header: Depth=1
	ldrsw	x9, [x0, x8, lsl #2]
	mul	x9, x3, x9
	str	x9, [x1, x8, lsl #3]
	add	x8, x8, #1
	cmp	w2, w8
	b.gt	LBB1_6
LBB1_7:
	ret
                                        ; -- End function
	.globl	_multiply_constant_int64_int32_neon ; -- Begin function multiply_constant_int64_int32_neon
	.p2align	2
_multiply_constant_int64_int32_neon:    ; @multiply_constant_int64_int32_neon
; %bb.0:
	cmp	w2, #4
	b.ge	LBB2_2
; %bb.1:
	mov	w8, #0                          ; =0x0
	cmp	w8, w2
	b.lt	LBB2_5
	b	LBB2_7
LBB2_2:
	fmov	s0, w3
	add	x8, x0, #16
	mov	w9, #4                          ; =0x4
	mov	x10, x1
	mov	w11, w2
LBB2_3:                                 ; =>This Inner Loop Header: Depth=1
	ldp	q1, q2, [x8, #-16]
	uzp1.4s	v1, v1, v2
	mul.4s	v1, v1, v0[0]
	str	q1, [x10], #16
	add	x9, x9, #4
	add	x8, x8, #32
	cmp	x9, x11
	b.ls	LBB2_3
; %bb.4:
	and	w8, w2, #0x7ffffffc
	cmp	w8, w2
	b.ge	LBB2_7
LBB2_5:
	mov	w8, w8
LBB2_6:                                 ; =>This Inner Loop Header: Depth=1
	ldr	x9, [x0, x8, lsl #3]
	mul	w9, w9, w3
	str	w9, [x1, x8, lsl #2]
	add	x8, x8, #1
	cmp	w2, w8
	b.gt	LBB2_6
LBB2_7:
	ret
                                        ; -- End function
	.globl	_multiply_constant_int64_int64_neon ; -- Begin function multiply_constant_int64_int64_neon
	.p2align	2
_multiply_constant_int64_int64_neon:    ; @multiply_constant_int64_int64_neon
; %bb.0:
	cmp	w2, #2
	b.ge	LBB3_2
; %bb.1:
	mov	w8, #0                          ; =0x0
	cmp	w8, w2
	b.lt	LBB3_5
	b	LBB3_7
LBB3_2:
	fmov	s0, w3
	lsr	x8, x3, #32
	fmov	s1, w8
	mov	w8, w2
	mov	w9, #2                          ; =0x2
	mov	x10, x0
	mov	x11, x1
LBB3_3:                                 ; =>This Inner Loop Header: Depth=1
	ldr	q2, [x10], #16
	xtn.2s	v3, v2
	ext.16b	v4, v2, v2, #8
	zip2.2s	v2, v2, v4
	umull.2d	v2, v2, v0[0]
	umlal.2d	v2, v3, v1[0]
	shl.2d	v2, v2, #32
	umlal.2d	v2, v3, v0[0]
	str	q2, [x11], #16
	add	x9, x9, #2
	cmp	x9, x8
	b.ls	LBB3_3
; %bb.4:
	and	w8, w2, #0x7ffffffe
	cmp	w8, w2
	b.ge	LBB3_7
LBB3_5:
	mov	w8, w8
LBB3_6:                                 ; =>This Inner Loop Header: Depth=1
	ldr	x9, [x0, x8, lsl #3]
	mul	x9, x9, x3
	str	x9, [x1, x8, lsl #3]
	add	x8, x8, #1
	cmp	w2, w8
	b.gt	LBB3_6
LBB3_7:
	ret
                                        ; -- End function
.subsections_via_symbols

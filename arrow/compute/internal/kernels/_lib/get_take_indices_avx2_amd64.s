	.intel_syntax noprefix
	.file	"get_take_indices_avx2_amd64.cc"
	.text
	.globl	get_take_indices_uint32_avx2
	.p2align	4
	.type	get_take_indices_uint32_avx2,@function
get_take_indices_uint32_avx2:           # @get_take_indices_uint32_avx2
# %bb.0:
	push	r14
	push	rbx
	test	rcx, rcx
	jle	.LBB0_10
# %bb.1:
	dec	rcx
	je	.LBB0_2
# %bb.11:
	vpxor	xmm0, xmm0, xmm0
	vmovdqu	xmm1, xmmword ptr [rdx + 384]
	xor	eax, eax
	vmovdqu	xmm2, xmmword ptr [rdx + 352]
	vmovdqu	xmm3, xmmword ptr [rdx + 368]
	xor	r9d, r9d
	jmp	.LBB0_12
	.p2align	4
.LBB0_14:                               #   in Loop: Header=BB0_12 Depth=1
	vpor	xmm4, xmm0, xmm2
	vpor	xmm5, xmm0, xmm3
	vmovdqu	xmmword ptr [rsi + 4*rax], xmm4
	vmovdqu	xmmword ptr [rsi + 4*rax + 16], xmm5
	add	rax, 8
.LBB0_19:                               #   in Loop: Header=BB0_12 Depth=1
	vpaddd	xmm0, xmm0, xmm1
	inc	r9
	cmp	rcx, r9
	je	.LBB0_3
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rdi + r9]
	test	r10d, r10d
	je	.LBB0_19
# %bb.13:                               #   in Loop: Header=BB0_12 Depth=1
	cmp	r10d, 255
	je	.LBB0_14
# %bb.15:                               #   in Loop: Header=BB0_12 Depth=1
	mov	r11d, r10d
	and	r11d, 15
	mov	r14d, r10d
	shr	r14d, 4
	movzx	ebx, byte ptr [rdx + r11 + 336]
	movzx	r11d, byte ptr [rdx + r14 + 336]
	test	rbx, rbx
	je	.LBB0_17
# %bb.16:                               #   in Loop: Header=BB0_12 Depth=1
	mov	r14d, r10d
	shl	r14b, 4
	movzx	r14d, r14b
	vpor	xmm4, xmm0, xmm2
	vpshufb	xmm4, xmm4, xmmword ptr [rdx + r14]
	mov	r14d, ebx
	shl	r14d, 4
	vmovdqu	xmm5, xmmword ptr [rdx + r14 + 256]
	vpmaskmovd	xmmword ptr [rsi + 4*rax], xmm5, xmm4
	add	rax, rbx
.LBB0_17:                               #   in Loop: Header=BB0_12 Depth=1
	test	r11, r11
	je	.LBB0_19
# %bb.18:                               #   in Loop: Header=BB0_12 Depth=1
	and	r10d, -16
	vpor	xmm4, xmm0, xmm3
	vpshufb	xmm4, xmm4, xmmword ptr [rdx + r10]
	mov	r10d, r11d
	shl	r10d, 4
	vmovdqu	xmm5, xmmword ptr [rdx + r10 + 256]
	vpmaskmovd	xmmword ptr [rsi + 4*rax], xmm5, xmm4
	add	rax, r11
	jmp	.LBB0_19
.LBB0_2:
	xor	eax, eax
	vpxor	xmm0, xmm0, xmm0
	xor	r9d, r9d
.LBB0_3:
	movzx	edi, byte ptr [rdi + r9]
	cmp	r9, rcx
	movzx	ecx, r8b
	mov	r8d, 255
	cmove	r8d, ecx
	and	r8d, edi
	je	.LBB0_10
# %bb.4:
	cmp	r8d, 255
	jne	.LBB0_6
# %bb.5:
	vpor	xmm1, xmm0, xmmword ptr [rdx + 352]
	vpor	xmm0, xmm0, xmmword ptr [rdx + 368]
	vmovdqu	xmmword ptr [rsi + 4*rax], xmm1
	vmovdqu	xmmword ptr [rsi + 4*rax + 16], xmm0
	jmp	.LBB0_10
.LBB0_6:
	mov	edi, r8d
	and	edi, 15
	movzx	ecx, r8b
	mov	r10d, ecx
	shr	r10d, 4
	movzx	r9d, byte ptr [rdx + rdi + 336]
	movzx	edi, byte ptr [rdx + r10 + 336]
	test	r9, r9
	je	.LBB0_8
# %bb.7:
	shl	r8b, 4
	movzx	r8d, r8b
	vpor	xmm1, xmm0, xmmword ptr [rdx + 352]
	vpshufb	xmm1, xmm1, xmmword ptr [rdx + r8]
	mov	r8d, r9d
	shl	r8d, 4
	vmovdqu	xmm2, xmmword ptr [rdx + r8 + 256]
	vpmaskmovd	xmmword ptr [rsi + 4*rax], xmm2, xmm1
	add	rax, r9
.LBB0_8:
	test	rdi, rdi
	je	.LBB0_10
# %bb.9:
	and	ecx, 240
	vpor	xmm0, xmm0, xmmword ptr [rdx + 368]
	vpshufb	xmm0, xmm0, xmmword ptr [rdx + rcx]
	shl	edi, 4
	vmovdqu	xmm1, xmmword ptr [rdx + rdi + 256]
	vpmaskmovd	xmmword ptr [rsi + 4*rax], xmm1, xmm0
.LBB0_10:
	pop	rbx
	pop	r14
	ret
.Lfunc_end0:
	.size	get_take_indices_uint32_avx2, .Lfunc_end0-get_take_indices_uint32_avx2
                                        # -- End function
	.ident	"Apple clang version 21.0.0 (clang-2100.1.1.101)"
	.section	".note.GNU-stack","",@progbits
	.addrsig

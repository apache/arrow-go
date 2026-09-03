	.intel_syntax noprefix
	.file	"filter_uint32.cc"
	.text
	.globl	filter_uint32_avx2              # -- Begin function filter_uint32_avx2
	.p2align	4
	.type	filter_uint32_avx2,@function
filter_uint32_avx2:                     # @filter_uint32_avx2
# %bb.0:
	lea	rax, [r8 + 7]
	test	r8, r8
	cmovns	rax, r8
	cmp	r8, 8
	jl	.LBB0_11
# %bb.1:
	push	rbp
	push	r14
	push	rbx
	sar	rax, 3
	xor	r8d, r8d
	xor	r9d, r9d
	jmp	.LBB0_2
	.p2align	4
.LBB0_4:                                #   in Loop: Header=BB0_2 Depth=1
	vmovdqu	ymm0, ymmword ptr [rdi]
	vmovdqu	ymmword ptr [rdx + 4*r8], ymm0
	add	r8, 8
.LBB0_9:                                #   in Loop: Header=BB0_2 Depth=1
	inc	r9
	add	rdi, 32
	cmp	rax, r9
	je	.LBB0_10
.LBB0_2:                                # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rsi + r9]
	test	r10d, r10d
	je	.LBB0_9
# %bb.3:                                #   in Loop: Header=BB0_2 Depth=1
	cmp	r10d, 255
	je	.LBB0_4
# %bb.5:                                #   in Loop: Header=BB0_2 Depth=1
	mov	r11d, r10d
	and	r11d, 15
	mov	r14d, r10d
	shr	r14d, 4
	movzx	ebx, byte ptr [rcx + r11 + 336]
	movzx	r11d, byte ptr [rcx + r14 + 336]
	vmovdqu	xmm0, xmmword ptr [rdi + 16]
	test	rbx, rbx
	je	.LBB0_7
# %bb.6:                                #   in Loop: Header=BB0_2 Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi]
	mov	ebp, r10d
	shl	bpl, 4
	movzx	r14d, bpl
	vpshufb	xmm1, xmm1, xmmword ptr [rcx + r14]
	mov	r14d, ebx
	shl	r14d, 4
	vmovdqu	xmm2, xmmword ptr [rcx + r14 + 256]
	vpmaskmovd	xmmword ptr [rdx + 4*r8], xmm2, xmm1
	add	r8, rbx
.LBB0_7:                                #   in Loop: Header=BB0_2 Depth=1
	test	r11, r11
	je	.LBB0_9
# %bb.8:                                #   in Loop: Header=BB0_2 Depth=1
	and	r10d, -16
	vpshufb	xmm0, xmm0, xmmword ptr [rcx + r10]
	mov	r10d, r11d
	shl	r10d, 4
	vmovdqu	xmm1, xmmword ptr [rcx + r10 + 256]
	vpmaskmovd	xmmword ptr [rdx + 4*r8], xmm1, xmm0
	add	r8, r11
	jmp	.LBB0_9
.LBB0_10:
	pop	rbx
	pop	r14
	pop	rbp
.LBB0_11:
	vzeroupper
	ret
.Lfunc_end0:
	.size	filter_uint32_avx2, .Lfunc_end0-filter_uint32_avx2
                                        # -- End function
	.ident	"Apple clang version 21.0.0 (clang-2100.1.1.101)"
	.section	".note.GNU-stack","",@progbits
	.addrsig

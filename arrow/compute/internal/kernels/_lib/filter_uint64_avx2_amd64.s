	.intel_syntax noprefix
	.file	"filter_uint64.cc"
	.text
	.globl	filter_uint64_avx2              # -- Begin function filter_uint64_avx2
	.p2align	4
	.type	filter_uint64_avx2,@function
filter_uint64_avx2:                     # @filter_uint64_avx2
# %bb.0:
	lea	rax, [r8 + 7]
	test	r8, r8
	cmovns	rax, r8
	cmp	r8, 8
	jl	.LBB0_11
# %bb.1:
	push	r14
	push	rbx
	sar	rax, 3
	add	rdi, 32
	xor	r8d, r8d
	xor	r9d, r9d
	jmp	.LBB0_2
	.p2align	4
.LBB0_4:                                #   in Loop: Header=BB0_2 Depth=1
	vmovdqu	ymm0, ymmword ptr [rdi - 32]
	vmovdqu	ymm1, ymmword ptr [rdi]
	vmovdqu	ymmword ptr [rdx + 8*r8], ymm0
	vmovdqu	ymmword ptr [rdx + 8*r8 + 32], ymm1
	add	r8, 8
.LBB0_9:                                #   in Loop: Header=BB0_2 Depth=1
	inc	r9
	add	rdi, 64
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
	mov	r14d, r10d
	and	r14d, 15
	shr	r10d, 4
	movzx	ebx, byte ptr [rcx + r14 + 672]
	movzx	r11d, byte ptr [rcx + r10 + 672]
	test	rbx, rbx
	je	.LBB0_7
# %bb.6:                                #   in Loop: Header=BB0_2 Depth=1
	shl	r14d, 5
	vmovdqu	ymm0, ymmword ptr [rcx + r14]
	vpermd	ymm0, ymm0, ymmword ptr [rdi - 32]
	mov	r14d, ebx
	shl	r14d, 5
	vmovdqu	ymm1, ymmword ptr [rcx + r14 + 512]
	vpmaskmovd	ymmword ptr [rdx + 8*r8], ymm1, ymm0
	add	r8, rbx
.LBB0_7:                                #   in Loop: Header=BB0_2 Depth=1
	test	r11, r11
	je	.LBB0_9
# %bb.8:                                #   in Loop: Header=BB0_2 Depth=1
	shl	r10d, 5
	vmovdqu	ymm0, ymmword ptr [rcx + r10]
	vpermd	ymm0, ymm0, ymmword ptr [rdi]
	mov	r10d, r11d
	shl	r10d, 5
	vmovdqu	ymm1, ymmword ptr [rcx + r10 + 512]
	vpmaskmovd	ymmword ptr [rdx + 8*r8], ymm1, ymm0
	add	r8, r11
	jmp	.LBB0_9
.LBB0_10:
	pop	rbx
	pop	r14
.LBB0_11:
	vzeroupper
	ret
.Lfunc_end0:
	.size	filter_uint64_avx2, .Lfunc_end0-filter_uint64_avx2
                                        # -- End function
	.ident	"Apple clang version 21.0.0 (clang-2100.1.1.101)"
	.section	".note.GNU-stack","",@progbits
	.addrsig

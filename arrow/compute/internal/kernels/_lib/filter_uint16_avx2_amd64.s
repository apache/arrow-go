	.intel_syntax noprefix
	.file	"filter_uint16.cc"
	.text
	.globl	filter_uint16_avx2              # -- Begin function filter_uint16_avx2
	.p2align	4
	.type	filter_uint16_avx2,@function
filter_uint16_avx2:                     # @filter_uint16_avx2
# %bb.0:
	lea	rax, [r8 + 7]
	test	r8, r8
	cmovns	rax, r8
	cmp	r8, 8
	jl	.LBB0_21
# %bb.1:
	sar	rax, 3
	xor	r8d, r8d
	xor	r9d, r9d
	jmp	.LBB0_2
	.p2align	4
.LBB0_4:                                #   in Loop: Header=BB0_2 Depth=1
	vmovdqu	xmm0, xmmword ptr [rdi]
	vmovdqu	xmmword ptr [rdx + 2*r8], xmm0
	add	r8, 8
.LBB0_20:                               #   in Loop: Header=BB0_2 Depth=1
	inc	r9
	add	rdi, 16
	cmp	rax, r9
	je	.LBB0_21
.LBB0_2:                                # =>This Inner Loop Header: Depth=1
	movzx	r11d, byte ptr [rsi + r9]
	test	r11d, r11d
	je	.LBB0_20
# %bb.3:                                #   in Loop: Header=BB0_2 Depth=1
	cmp	r11d, 255
	je	.LBB0_4
# %bb.5:                                #   in Loop: Header=BB0_2 Depth=1
	movzx	r10d, byte ptr [rcx + r11 + 4096]
	shl	r11d, 4
	vmovdqu	xmm0, xmmword ptr [rdi]
	vpshufb	xmm0, xmm0, xmmword ptr [rcx + r11]
	cmp	r10, 4
	jb	.LBB0_13
# %bb.6:                                #   in Loop: Header=BB0_2 Depth=1
	vmovq	qword ptr [rdx + 2*r8], xmm0
	cmp	r10b, 7
	jb	.LBB0_8
# %bb.7:                                #   in Loop: Header=BB0_2 Depth=1
	vpextrw	word ptr [rdx + 2*r8 + 12], xmm0, 6
	jmp	.LBB0_10
.LBB0_13:                               #   in Loop: Header=BB0_2 Depth=1
	cmp	r10d, 3
	jne	.LBB0_15
# %bb.14:                               #   in Loop: Header=BB0_2 Depth=1
	vpextrw	word ptr [rdx + 2*r8 + 4], xmm0, 2
	vpextrw	word ptr [rdx + 2*r8 + 2], xmm0, 1
	jmp	.LBB0_18
.LBB0_8:                                #   in Loop: Header=BB0_2 Depth=1
	cmp	r10, 4
	je	.LBB0_12
# %bb.9:                                #   in Loop: Header=BB0_2 Depth=1
	cmp	r10d, 6
	jne	.LBB0_11
.LBB0_10:                               #   in Loop: Header=BB0_2 Depth=1
	vpextrw	word ptr [rdx + 2*r8 + 10], xmm0, 5
.LBB0_11:                               #   in Loop: Header=BB0_2 Depth=1
	vpextrw	word ptr [rdx + 2*r8 + 8], xmm0, 4
.LBB0_12:                               #   in Loop: Header=BB0_2 Depth=1
	add	r8, 4
	add	r10d, -4
	jmp	.LBB0_19
.LBB0_15:                               #   in Loop: Header=BB0_2 Depth=1
	cmp	r10b, 2
	jb	.LBB0_17
# %bb.16:                               #   in Loop: Header=BB0_2 Depth=1
	vpextrw	word ptr [rdx + 2*r8 + 2], xmm0, 1
	jmp	.LBB0_18
.LBB0_17:                               #   in Loop: Header=BB0_2 Depth=1
	test	r10, r10
	je	.LBB0_19
.LBB0_18:                               #   in Loop: Header=BB0_2 Depth=1
	vpextrw	word ptr [rdx + 2*r8], xmm0, 0
.LBB0_19:                               #   in Loop: Header=BB0_2 Depth=1
	add	r8, r10
	jmp	.LBB0_20
.LBB0_21:
	ret
.Lfunc_end0:
	.size	filter_uint16_avx2, .Lfunc_end0-filter_uint16_avx2
                                        # -- End function
	.ident	"Apple clang version 21.0.0 (clang-2100.1.1.101)"
	.section	".note.GNU-stack","",@progbits
	.addrsig

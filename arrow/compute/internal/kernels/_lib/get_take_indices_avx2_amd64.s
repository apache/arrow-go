	.text
	.intel_syntax noprefix
	.file	"get_take_indices_avx2_amd64.cc"
	.globl	get_take_indices_uint32_avx2    # -- Begin function get_take_indices_uint32_avx2
	.p2align	4, 0x90
	.type	get_take_indices_uint32_avx2,@function
get_take_indices_uint32_avx2:           # @get_take_indices_uint32_avx2
# %bb.0:
	test	rcx, rcx
	jle	.LBB0_19
# %bb.1:
	vmovdqu	xmm2, xmmword ptr [rdx + 352]
	vmovdqu	xmm0, xmmword ptr [rdx + 368]
	lea	rax, [rcx - 1]
	vpxor	xmm1, xmm1, xmm1
	cmp	rcx, 1
	jne	.LBB0_5
.LBB0_2:
	movzx	eax, byte ptr [rdi + rax]
	and	r8d, eax
	je	.LBB0_19
# %bb.3:
	cmp	r8d, 255
	jne	.LBB0_15
# %bb.4:
	vpor	xmm2, xmm1, xmm2
	vpor	xmm0, xmm1, xmm0
	vmovdqu	xmmword ptr [rsi], xmm2
	vmovdqu	xmmword ptr [rsi + 16], xmm0
	jmp	.LBB0_19
.LBB0_5:
	vmovdqu	xmm3, xmmword ptr [rdx + 384]
	xor	ecx, ecx
	jmp	.LBB0_9
	.p2align	4, 0x90
.LBB0_6:                                #   in Loop: Header=BB0_9 Depth=1
	vpor	xmm4, xmm1, xmm2
	vpor	xmm5, xmm1, xmm0
	vmovdqu	xmmword ptr [rsi], xmm4
	vmovdqu	xmmword ptr [rsi + 16], xmm5
	mov	r10d, 8
.LBB0_7:                                #   in Loop: Header=BB0_9 Depth=1
	lea	rsi, [rsi + 4*r10]
.LBB0_8:                                #   in Loop: Header=BB0_9 Depth=1
	vpaddd	xmm1, xmm3, xmm1
	inc	rcx
	cmp	rax, rcx
	je	.LBB0_2
.LBB0_9:                                # =>This Inner Loop Header: Depth=1
	movzx	r9d, byte ptr [rdi + rcx]
	test	r9d, r9d
	je	.LBB0_8
# %bb.10:                               #   in Loop: Header=BB0_9 Depth=1
	cmp	r9d, 255
	je	.LBB0_6
# %bb.11:                               #   in Loop: Header=BB0_9 Depth=1
	mov	r11d, r9d
	and	r11d, 15
	movzx	r10d, byte ptr [rdx + r11 + 336]
	test	r10, r10
	je	.LBB0_13
# %bb.12:                               #   in Loop: Header=BB0_9 Depth=1
	shl	r11d, 4
	vpor	xmm4, xmm1, xmm2
	vpshufb	xmm4, xmm4, xmmword ptr [rdx + r11]
	mov	r11d, r10d
	shl	r11d, 4
	vmovdqu	xmm5, xmmword ptr [rdx + r11 + 256]
	vpmaskmovd	xmmword ptr [rsi], xmm5, xmm4
	lea	rsi, [rsi + 4*r10]
.LBB0_13:                               #   in Loop: Header=BB0_9 Depth=1
	mov	r10d, r9d
	shr	r10d, 4
	movzx	r10d, byte ptr [rdx + r10 + 336]
	test	r10, r10
	je	.LBB0_8
# %bb.14:                               #   in Loop: Header=BB0_9 Depth=1
	and	r9d, -16
	vpor	xmm4, xmm1, xmm0
	vpshufb	xmm4, xmm4, xmmword ptr [rdx + r9]
	mov	r9d, r10d
	shl	r9d, 4
	vmovdqu	xmm5, xmmword ptr [rdx + r9 + 256]
	vpmaskmovd	xmmword ptr [rsi], xmm5, xmm4
	jmp	.LBB0_7
.LBB0_15:
	mov	ecx, r8d
	and	ecx, 15
	movzx	eax, byte ptr [rdx + rcx + 336]
	test	rax, rax
	je	.LBB0_17
# %bb.16:
	shl	ecx, 4
	vpor	xmm2, xmm1, xmm2
	vpshufb	xmm2, xmm2, xmmword ptr [rdx + rcx]
	mov	ecx, eax
	shl	ecx, 4
	vmovdqu	xmm3, xmmword ptr [rdx + rcx + 256]
	vpmaskmovd	xmmword ptr [rsi], xmm3, xmm2
	lea	rsi, [rsi + 4*rax]
.LBB0_17:
	mov	eax, r8d
	shr	eax, 4
	movzx	eax, byte ptr [rdx + rax + 336]
	test	rax, rax
	je	.LBB0_19
# %bb.18:
	and	r8d, -16
	vpor	xmm0, xmm1, xmm0
	vpshufb	xmm0, xmm0, xmmword ptr [rdx + r8]
	shl	eax, 4
	vmovdqu	xmm1, xmmword ptr [rdx + rax + 256]
	vpmaskmovd	xmmword ptr [rsi], xmm1, xmm0
.LBB0_19:
	ret
.Lfunc_end0:
	.size	get_take_indices_uint32_avx2, .Lfunc_end0-get_take_indices_uint32_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 18.1.3 (1)"
	.section	".note.GNU-stack","",@progbits
	.addrsig

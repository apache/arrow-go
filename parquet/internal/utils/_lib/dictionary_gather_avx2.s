	.intel_syntax noprefix
	.file	"dictionary_gather_avx2.c"
	.text
	.globl	dictionary_gather_32_avx2       # -- Begin function dictionary_gather_32_avx2
	.p2align	4
	.type	dictionary_gather_32_avx2,@function
dictionary_gather_32_avx2:              # @dictionary_gather_32_avx2
# %bb.0:
                                        # kill: def $ecx killed $ecx def $rcx
	xor	r8d, r8d
	mov	eax, ecx
	cmp	ecx, 8
	jl	.LBB0_4
# %bb.1:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	xor	r8d, r8d
	.p2align	4
.LBB0_2:                                # =>This Inner Loop Header: Depth=1
	mov	r9, r8
	vmovdqu	ymm0, ymmword ptr [rdx + 4*r8]
	vpcmpeqd	ymm1, ymm1, ymm1
	vpxor	xmm2, xmm2, xmm2
	vpgatherdd	ymm2, dword ptr [rdi + 4*ymm0], ymm1
	vmovdqu	ymmword ptr [rsi + 4*r8], ymm2
	add	r8, 8
	add	r9, 16
	cmp	r9, rax
	jbe	.LBB0_2
# %bb.3:
	mov	rsp, rbp
	pop	rbp
.LBB0_4:
	cmp	r8d, ecx
	jge	.LBB0_10
# %bb.5:
	mov	r9d, r8d
	sub	ecx, r8d
	mov	r8, r9
	and	ecx, 3
	je	.LBB0_8
# %bb.6:
	mov	r8, r9
	.p2align	4
.LBB0_7:                                # =>This Inner Loop Header: Depth=1
	movsxd	r10, dword ptr [rdx + 4*r8]
	mov	r10d, dword ptr [rdi + 4*r10]
	mov	dword ptr [rsi + 4*r8], r10d
	inc	r8
	dec	rcx
	jne	.LBB0_7
.LBB0_8:
	sub	r9, rax
	cmp	r9, -4
	ja	.LBB0_10
	.p2align	4
.LBB0_9:                                # =>This Inner Loop Header: Depth=1
	movsxd	rcx, dword ptr [rdx + 4*r8]
	mov	ecx, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*r8], ecx
	movsxd	rcx, dword ptr [rdx + 4*r8 + 4]
	mov	ecx, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*r8 + 4], ecx
	movsxd	rcx, dword ptr [rdx + 4*r8 + 8]
	mov	ecx, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*r8 + 8], ecx
	movsxd	rcx, dword ptr [rdx + 4*r8 + 12]
	mov	ecx, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*r8 + 12], ecx
	add	r8, 4
	cmp	rax, r8
	jne	.LBB0_9
.LBB0_10:
	vzeroupper
	ret
.Lfunc_end0:
	.size	dictionary_gather_32_avx2, .Lfunc_end0-dictionary_gather_32_avx2
                                        # -- End function
	.globl	dictionary_gather_64_avx2       # -- Begin function dictionary_gather_64_avx2
	.p2align	4
	.type	dictionary_gather_64_avx2,@function
dictionary_gather_64_avx2:              # @dictionary_gather_64_avx2
# %bb.0:
                                        # kill: def $ecx killed $ecx def $rcx
	xor	r8d, r8d
	mov	eax, ecx
	cmp	ecx, 4
	jl	.LBB1_4
# %bb.1:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	xor	r8d, r8d
	.p2align	4
.LBB1_2:                                # =>This Inner Loop Header: Depth=1
	mov	r9, r8
	vmovdqu	xmm0, xmmword ptr [rdx + 4*r8]
	vpcmpeqd	ymm1, ymm1, ymm1
	vpxor	xmm2, xmm2, xmm2
	vpgatherdq	ymm2, qword ptr [rdi + 8*xmm0], ymm1
	vmovdqu	ymmword ptr [rsi + 8*r8], ymm2
	add	r8, 4
	add	r9, 8
	cmp	r9, rax
	jbe	.LBB1_2
# %bb.3:
	mov	rsp, rbp
	pop	rbp
.LBB1_4:
	cmp	r8d, ecx
	jge	.LBB1_10
# %bb.5:
	mov	r9d, r8d
	sub	ecx, r8d
	mov	r8, r9
	and	ecx, 3
	je	.LBB1_8
# %bb.6:
	mov	r8, r9
	.p2align	4
.LBB1_7:                                # =>This Inner Loop Header: Depth=1
	movsxd	r10, dword ptr [rdx + 4*r8]
	mov	r10, qword ptr [rdi + 8*r10]
	mov	qword ptr [rsi + 8*r8], r10
	inc	r8
	dec	rcx
	jne	.LBB1_7
.LBB1_8:
	sub	r9, rax
	cmp	r9, -4
	ja	.LBB1_10
	.p2align	4
.LBB1_9:                                # =>This Inner Loop Header: Depth=1
	movsxd	rcx, dword ptr [rdx + 4*r8]
	mov	rcx, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*r8], rcx
	movsxd	rcx, dword ptr [rdx + 4*r8 + 4]
	mov	rcx, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*r8 + 8], rcx
	movsxd	rcx, dword ptr [rdx + 4*r8 + 8]
	mov	rcx, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*r8 + 16], rcx
	movsxd	rcx, dword ptr [rdx + 4*r8 + 12]
	mov	rcx, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*r8 + 24], rcx
	add	r8, 4
	cmp	rax, r8
	jne	.LBB1_9
.LBB1_10:
	vzeroupper
	ret
.Lfunc_end1:
	.size	dictionary_gather_64_avx2, .Lfunc_end1-dictionary_gather_64_avx2
                                        # -- End function
	.ident	"Apple clang version 21.0.0 (clang-2100.1.1.101)"
	.section	".note.GNU-stack","",@progbits
	.addrsig

	.text
	.intel_syntax noprefix
	.file	"bloom_filter_block.c"
	.globl	check_block_sse4                # -- Begin function check_block_sse4
	.p2align	4, 0x90
	.type	check_block_sse4,@function
check_block_sse4:                       # @check_block_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
                                        # kill: def $esi killed $esi def $rsi
	mov	rcx, rdx
	shr	rcx, 32
	lea	eax, [rsi + 7]
	test	esi, esi
	cmovns	eax, esi
	sar	eax, 3
	cdqe
	imul	rax, rcx
	shr	rax, 29
	and	eax, -8
	imul	ecx, edx, 1203114875
	shr	ecx, 27
	mov	esi, dword ptr [rdi + 4*rax]
	bt	esi, ecx
	jae	.LBB0_8
# %bb.1:
	imul	ecx, edx, 1150766481
	shr	ecx, 27
	mov	esi, dword ptr [rdi + 4*rax + 4]
	bt	esi, ecx
	jae	.LBB0_8
# %bb.2:
	imul	ecx, edx, -2010862245
	shr	ecx, 27
	mov	esi, dword ptr [rdi + 4*rax + 8]
	bt	esi, ecx
	jae	.LBB0_8
# %bb.3:
	imul	ecx, edx, -1565054819
	shr	ecx, 27
	mov	esi, dword ptr [rdi + 4*rax + 12]
	bt	esi, ecx
	jae	.LBB0_8
# %bb.4:
	imul	ecx, edx, 1884591559
	shr	ecx, 27
	mov	esi, dword ptr [rdi + 4*rax + 16]
	bt	esi, ecx
	jae	.LBB0_8
# %bb.5:
	imul	ecx, edx, 770785867
	shr	ecx, 27
	mov	esi, dword ptr [rdi + 4*rax + 20]
	bt	esi, ecx
	jae	.LBB0_8
# %bb.6:
	imul	ecx, edx, -1627633337
	shr	ecx, 27
	mov	esi, dword ptr [rdi + 4*rax + 24]
	bt	esi, ecx
	jae	.LBB0_8
# %bb.7:
	imul	ecx, edx, 1550580529
	shr	ecx, 27
	mov	eax, dword ptr [rdi + 4*rax + 28]
	bt	eax, ecx
	setb	al
                                        # kill: def $al killed $al killed $eax
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_8:
	xor	eax, eax
                                        # kill: def $al killed $al killed $eax
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	check_block_sse4, .Lfunc_end0-check_block_sse4
                                        # -- End function
	.globl	check_bulk_sse4                 # -- Begin function check_bulk_sse4
	.p2align	4, 0x90
	.type	check_bulk_sse4,@function
check_bulk_sse4:                        # @check_bulk_sse4
# %bb.0:
                                        # kill: def $esi killed $esi def $rsi
	test	r8d, r8d
	jle	.LBB1_19
# %bb.1:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	lea	eax, [rsi + 7]
	test	esi, esi
	cmovns	eax, esi
	sar	eax, 3
	cdqe
	mov	esi, r8d
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB1_4:                                # =>This Inner Loop Header: Depth=1
	mov	r10, qword ptr [rdx + 8*r8]
	mov	r9, r10
	shr	r9, 32
	imul	r9, rax
	shr	r9, 29
	and	r9d, -8
	imul	r11d, r10d, 1203114875
	shr	r11d, 27
	mov	ebx, dword ptr [rdi + 4*r9]
	bt	ebx, r11d
	jae	.LBB1_2
# %bb.5:                                #   in Loop: Header=BB1_4 Depth=1
	imul	r11d, r10d, 1150766481
	shr	r11d, 27
	mov	ebx, dword ptr [rdi + 4*r9 + 4]
	bt	ebx, r11d
	jae	.LBB1_2
# %bb.6:                                #   in Loop: Header=BB1_4 Depth=1
	imul	r11d, r10d, -2010862245
	shr	r11d, 27
	mov	ebx, dword ptr [rdi + 4*r9 + 8]
	bt	ebx, r11d
	jae	.LBB1_2
# %bb.7:                                #   in Loop: Header=BB1_4 Depth=1
	imul	r11d, r10d, -1565054819
	shr	r11d, 27
	mov	ebx, dword ptr [rdi + 4*r9 + 12]
	bt	ebx, r11d
	jae	.LBB1_2
# %bb.8:                                #   in Loop: Header=BB1_4 Depth=1
	imul	r11d, r10d, 1884591559
	shr	r11d, 27
	mov	ebx, dword ptr [rdi + 4*r9 + 16]
	bt	ebx, r11d
	jae	.LBB1_2
# %bb.9:                                #   in Loop: Header=BB1_4 Depth=1
	imul	r11d, r10d, 770785867
	shr	r11d, 27
	mov	ebx, dword ptr [rdi + 4*r9 + 20]
	bt	ebx, r11d
	jae	.LBB1_2
# %bb.10:                               #   in Loop: Header=BB1_4 Depth=1
	imul	r11d, r10d, -1627633337
	shr	r11d, 27
	mov	ebx, dword ptr [rdi + 4*r9 + 24]
	bt	ebx, r11d
	jae	.LBB1_2
# %bb.11:                               #   in Loop: Header=BB1_4 Depth=1
	imul	r10d, r10d, 1550580529
	shr	r10d, 27
	mov	r9d, dword ptr [rdi + 4*r9 + 28]
	bt	r9d, r10d
	setb	r9b
	mov	byte ptr [rcx + r8], r9b
	inc	r8
	cmp	rsi, r8
	jne	.LBB1_4
	jmp	.LBB1_18
	.p2align	4, 0x90
.LBB1_2:                                #   in Loop: Header=BB1_4 Depth=1
	xor	r9d, r9d
	mov	byte ptr [rcx + r8], r9b
	inc	r8
	cmp	rsi, r8
	jne	.LBB1_4
.LBB1_18:
	# lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
.LBB1_19:
	ret
.Lfunc_end1:
	.size	check_bulk_sse4, .Lfunc_end1-check_bulk_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4, 0x0                          # -- Begin function insert_block_sse4
.LCPI2_0:
	.long	1203114875                      # 0x47b6137b
	.long	1150766481                      # 0x44974d91
	.long	2284105051                      # 0x8824ad5b
	.long	2729912477                      # 0xa2b7289d
.LCPI2_1:
	.long	1065353216                      # 0x3f800000
	.long	1065353216                      # 0x3f800000
	.long	1065353216                      # 0x3f800000
	.long	1065353216                      # 0x3f800000
.LCPI2_2:
	.long	1884591559                      # 0x705495c7
	.long	770785867                       # 0x2df1424b
	.long	2667333959                      # 0x9efc4947
	.long	1550580529                      # 0x5c6bfb31
	.text
	.globl	insert_block_sse4
	.p2align	4, 0x90
	.type	insert_block_sse4,@function
insert_block_sse4:                      # @insert_block_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
                                        # kill: def $esi killed $esi def $rsi
	movd	xmm0, edx
	shr	rdx, 32
	lea	eax, [rsi + 7]
	test	esi, esi
	cmovns	eax, esi
	sar	eax, 3
	movsxd	rcx, eax
	imul	rcx, rdx
	shr	rcx, 27
	movabs	rax, 17179869152
	and	rax, rcx
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	movdqa	xmm1, xmmword ptr [rip + .LCPI2_0] # xmm1 = [1203114875,1150766481,2284105051,2729912477]
	pmulld	xmm1, xmm0
	psrld	xmm1, 27
	pslld	xmm1, 23
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_1] # xmm2 = [1065353216,1065353216,1065353216,1065353216]
	paddd	xmm1, xmm2
	cvttps2dq	xmm1, xmm1
	movups	xmm3, xmmword ptr [rdi + rax]
	orps	xmm3, xmm1
	movups	xmm1, xmmword ptr [rdi + rax + 16]
	movups	xmmword ptr [rdi + rax], xmm3
	pmulld	xmm0, xmmword ptr [rip + .LCPI2_2]
	psrld	xmm0, 27
	pslld	xmm0, 23
	paddd	xmm0, xmm2
	cvttps2dq	xmm0, xmm0
	orps	xmm0, xmm1
	movups	xmmword ptr [rdi + rax + 16], xmm0
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end2:
	.size	insert_block_sse4, .Lfunc_end2-insert_block_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4, 0x0                          # -- Begin function insert_bulk_sse4
.LCPI3_0:
	.long	1203114875                      # 0x47b6137b
	.long	1150766481                      # 0x44974d91
	.long	2284105051                      # 0x8824ad5b
	.long	2729912477                      # 0xa2b7289d
.LCPI3_1:
	.long	1065353216                      # 0x3f800000
	.long	1065353216                      # 0x3f800000
	.long	1065353216                      # 0x3f800000
	.long	1065353216                      # 0x3f800000
.LCPI3_2:
	.long	1884591559                      # 0x705495c7
	.long	770785867                       # 0x2df1424b
	.long	2667333959                      # 0x9efc4947
	.long	1550580529                      # 0x5c6bfb31
	.text
	.globl	insert_bulk_sse4
	.p2align	4, 0x90
	.type	insert_bulk_sse4,@function
insert_bulk_sse4:                       # @insert_bulk_sse4
# %bb.0:
                                        # kill: def $esi killed $esi def $rsi
	test	ecx, ecx
	jle	.LBB3_4
# %bb.1:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	lea	eax, [rsi + 7]
	test	esi, esi
	cmovns	eax, esi
	sar	eax, 3
	cdqe
	mov	ecx, ecx
	xor	esi, esi
	movabs	r8, 17179869152
	movdqa	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [1203114875,1150766481,2284105051,2729912477]
	movdqa	xmm1, xmmword ptr [rip + .LCPI3_1] # xmm1 = [1065353216,1065353216,1065353216,1065353216]
	movdqa	xmm2, xmmword ptr [rip + .LCPI3_2] # xmm2 = [1884591559,770785867,2667333959,1550580529]
	.p2align	4, 0x90
.LBB3_2:                                # =>This Inner Loop Header: Depth=1
	mov	r9, qword ptr [rdx + 8*rsi]
	movd	xmm3, r9d
	shr	r9, 32
	imul	r9, rax
	shr	r9, 27
	and	r9, r8
	pshufd	xmm3, xmm3, 0                   # xmm3 = xmm3[0,0,0,0]
	movdqa	xmm4, xmm3
	pmulld	xmm4, xmm0
	psrld	xmm4, 27
	pslld	xmm4, 23
	paddd	xmm4, xmm1
	cvttps2dq	xmm4, xmm4
	movups	xmm5, xmmword ptr [rdi + r9]
	orps	xmm5, xmm4
	movups	xmm4, xmmword ptr [rdi + r9 + 16]
	movups	xmmword ptr [rdi + r9], xmm5
	pmulld	xmm3, xmm2
	psrld	xmm3, 27
	pslld	xmm3, 23
	paddd	xmm3, xmm1
	cvttps2dq	xmm3, xmm3
	orps	xmm3, xmm4
	movups	xmmword ptr [rdi + r9 + 16], xmm3
	inc	rsi
	cmp	rcx, rsi
	jne	.LBB3_2
# %bb.3:
	mov	rsp, rbp
	pop	rbp
.LBB3_4:
	ret
.Lfunc_end3:
	.size	insert_bulk_sse4, .Lfunc_end3-insert_bulk_sse4
                                        # -- End function
	.ident	"clang version 19.1.6 (https://github.com/conda-forge/clangdev-feedstock a097c63bb6a9919682224023383a143d482c552e)"
	.section	".note.GNU-stack","",@progbits
	.addrsig
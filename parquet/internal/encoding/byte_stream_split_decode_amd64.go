package encoding

import "golang.org/x/sys/cpu"

func init() {
	if cpu.X86.HasAVX2 {
		decodeByteStreamSplitBatchWidth4SIMD = decodeByteStreamSplitBatchWidth4AVX2
	}
}

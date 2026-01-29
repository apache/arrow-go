//go:build !noasm
// +build !noasm

package encoding

import (
	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.ARM64.HasASIMD {
		decodeByteStreamSplitBatchWidth4SIMD = decodeByteStreamSplitBatchWidth4NEON
	}
}

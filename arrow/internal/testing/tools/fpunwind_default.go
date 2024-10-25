//go:build !arm64

package tools

// FPUnwind does frame pointer unwinding. It is implemented in assembly.
// If frame pointers are broken, it will crash.
// It is currently only implemented for arm64
func FPUnwind() {}

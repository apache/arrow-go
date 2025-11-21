//go:build refcounting

package memory

import (
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/internal/debug"
)

type Refcount struct {
	count        atomic.Int64
	Dependencies []**Refcount
	Buffers []**Buffer
	Derived []unsafe.Pointer
}

func (r *Refcount) ReferenceDependency(d ...**Refcount) {
	if r.Dependencies == nil {
		r.Dependencies = d
	} else {
		for _, d := range d {
			r.Dependencies = append(r.Dependencies, d)
		}
	}
}

func (r *Refcount) ReferenceBuffer(b...**Buffer) {
	if r.Buffers == nil {
		r.Buffers = b
	} else {
		for _, b := range b {
			r.Buffers = append(r.Buffers, b)
		}
	}
}


func (r *Refcount) ReferenceDerived(p ...unsafe.Pointer) {
	if r.Derived == nil {
		r.Derived = p
	} else {
		for _, p := range p {
			r.Derived = append(r.Derived, p)
		}
	}
}

func (r *Refcount) Retain() {
	r.count.Add(1)
}

func (r *Refcount) Release() {
	new := r.count.Add(-1)
	if new == 0 {
		for _, buffer := range r.Buffers {
			(*buffer).Release()
			*buffer = nil
		}
		for _, dependency := range r.Dependencies {
			(*dependency).Release()
			*dependency = nil
		}
		r.Buffers = nil
		r.Dependencies = nil
		for _, derived := range r.Derived {
			*((*uintptr)(derived)) = 0
		}
	} else if new < 0 {
		// This branch can be optimized out when !debug
		// This avoids an unnecessary extra Load
		debug.Assert(false, "too many releases")
	}
}

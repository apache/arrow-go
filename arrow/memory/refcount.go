//go:build refcounting

package memory

import (
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/internal/debug"
)

type Refcount struct {
	count        atomic.Int64
	dependencies []**Refcount
	buffers      []**Buffer
	derived      []unsafe.Pointer
}

// Must only be called once per object. Defines the dependency tree.
// When this object is completely unreferenced, all dependencies will
// be unreferenced by it and, if this was the only object still
// referencing them, they will be freed as well, recursively.
func (r *Refcount) ReferenceDependency(d ...**Refcount) {
	r.dependencies = d
}

// Must only be called once per object. Defines buffers that are referenced
// by this object. When this object is unreferenced, all such buffers will
// be deallocated immediately.
func (r *Refcount) ReferenceBuffer(b ...**Buffer) {
	r.buffers = b
}

// Must only be called once per object, with a list of pointers that are
// _derived from_ allocations owned by or referenced by this object.
// When this object is unreferenced, all such pointers will be nilled.
// Note: this needs the _address of_ the pointers to nil, _not_ the pointers
// themselves!
func (r *Refcount) ReferenceDerived(p ...unsafe.Pointer) {
	r.derived = p
}

func (r *Refcount) Retain() {
	r.count.Add(1)
}

func (r *Refcount) Release() {
	new := r.count.Add(-1)
	if new == 0 {
		for _, buffer := range r.buffers {
			(*buffer).Release()
			*buffer = nil
		}
		for _, dependency := range r.dependencies {
			(*dependency).Release()
			*dependency = nil
		}
		r.buffers = nil
		r.dependencies = nil
		for _, derived := range r.derived {
			*((*uintptr)(derived)) = 0
		}
	} else if new < 0 {
		// This branch can be optimized out when !debug
		// This avoids an unnecessary extra Load
		debug.Assert(false, "too many releases")
	}
}

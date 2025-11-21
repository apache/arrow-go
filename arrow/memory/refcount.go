package memory

import (
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/internal/debug"
)

type Refcount struct {
	count        atomic.Int64
	Dependencies []**Refcount
	Buffers []**Buffer
	Additional func()
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
		if r.Additional != nil {
			r.Additional()
		}
	} else if new < 0 {
		// This branch can be optimized out when !debug
		// This avoids an unnecessary extra Load
		debug.Assert(false, "too many releases")
	}
}

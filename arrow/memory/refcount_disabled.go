//go:build !refcounting

package memory

type Refcount struct{}

func (r *Refcount) ReferenceDependency(d ...any) {}
func (r *Refcount) ReferenceBuffer(b ...any) {}
func (r *Refcount) ReferenceDerived(p ...any) {}
func (r *Refcount) Retain() {}
func (r *Refcount) Release() {}

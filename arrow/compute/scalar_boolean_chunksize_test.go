package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// and_kleene and or_kleene over two boolean arrays with nulls, executed with
// different ExecCtx.ChunkSize values; the result must not depend on ChunkSize.
func TestKleeneChunkSizeIndependent(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// left:  [true, null, true, null, false, true, null, true]
	// right: [null, true, true, false, null, null, true, true]
	lb := array.NewBooleanBuilder(mem)
	lb.AppendValues([]bool{true, false, true, false, false, true, false, true}, []bool{true, false, true, false, true, true, false, true})
	left := lb.NewArray()
	lb.Release()
	defer left.Release()
	rb := array.NewBooleanBuilder(mem)
	rb.AppendValues([]bool{false, true, true, false, false, false, true, true}, []bool{false, true, true, true, false, false, true, true})
	right := rb.NewArray()
	rb.Release()
	defer right.Release()

	for _, fn := range []string{"and_kleene", "or_kleene"} {
		t.Run(fn, func(t *testing.T) { checkChunkSizeIndependent(t, mem, fn, left, right) })
	}
}

func checkChunkSizeIndependent(t *testing.T, mem memory.Allocator, fn string, left, right arrow.Array) {
	run := func(chunk int64) string {
		ectx := compute.DefaultExecCtx()
		ectx.ChunkSize = chunk
		ctx := compute.SetExecCtx(compute.WithAllocator(context.Background(), mem), ectx)
		out, err := compute.CallFunction(ctx, fn, nil, &compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()})
		if err != nil {
			t.Fatal(err)
		}
		defer out.Release()
		arr := out.(*compute.ArrayDatum).MakeArray()
		defer arr.Release()
		return arr.String()
	}
	ref := run(compute.DefaultMaxChunkSize)
	t.Logf("ChunkSize default: %s", ref)
	for _, n := range []int64{1, 2, 3, 4, 8} {
		got := run(n)
		t.Logf("ChunkSize %d: %s", n, got)
		if got != ref {
			t.Errorf("ChunkSize %d: got %s, want %s", n, got, ref)
		}
	}
}

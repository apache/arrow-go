package encoding

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func TestPutDictionary(t *testing.T) {
	exp := []int32{1, 2, 4, 8, 16}
	ad := array.NewData(
		arrow.PrimitiveTypes.Int32, len(exp),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(exp))},
		nil, 0, 0,
	)
	arr := array.NewInt32Data(ad)

	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(typ, 0, 0)
	enc := &typedDictEncoder[int32]{newDictEncoderBase(descr, NewDictionary[int32](), memory.DefaultAllocator)}

	err := enc.PutDictionary(arr)
	assert.NoError(t, err)
}

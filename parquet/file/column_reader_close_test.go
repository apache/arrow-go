// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package file

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	format "github.com/apache/arrow-go/v18/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/require"
)

type closeTrackingPage struct {
	releaseCalls int
}

func (p *closeTrackingPage) Type() PageType            { return PageTypeDataPage }
func (p *closeTrackingPage) Data() []byte              { return nil }
func (p *closeTrackingPage) Encoding() format.Encoding { return format.Encoding_PLAIN }
func (p *closeTrackingPage) NumValues() int32          { return 0 }
func (p *closeTrackingPage) Release()                  { p.releaseCalls++ }

type closeTrackingPageReader struct {
	closeCalls int
	closeErr   error
}

func (r *closeTrackingPageReader) SetMaxPageHeaderSize(int) {}
func (r *closeTrackingPageReader) Page() Page               { return nil }
func (r *closeTrackingPageReader) Next() bool               { return false }
func (r *closeTrackingPageReader) Err() error               { return nil }
func (r *closeTrackingPageReader) Reset(parquet.BufferedReader, int64, compress.Compression, *CryptoContext) {
}
func (r *closeTrackingPageReader) GetDictionaryPage() (*DictionaryPage, error) { return nil, nil }
func (r *closeTrackingPageReader) SeekToPageWithRow(int64) error               { return nil }
func (r *closeTrackingPageReader) Close() error {
	r.closeCalls++
	return r.closeErr
}

func TestColumnChunkReaderCloseIsIdempotent(t *testing.T) {
	page := &closeTrackingPage{}
	rdr := &closeTrackingPageReader{}
	reader := &columnChunkReader{curPage: page, rdr: rdr}

	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
	require.Equal(t, 1, page.releaseCalls)
	require.Equal(t, 1, rdr.closeCalls)
	require.Nil(t, reader.curPage)
	require.Nil(t, reader.rdr)
}

func TestColumnChunkReaderSetPageReaderReleasesBeforeReplacement(t *testing.T) {
	page := &closeTrackingPage{}
	oldRdr := &closeTrackingPageReader{}
	newRdr := &closeTrackingPageReader{}
	reader := &columnChunkReader{curPage: page, rdr: oldRdr}

	require.NoError(t, reader.setPageReader(newRdr))
	require.Equal(t, 1, page.releaseCalls)
	require.Equal(t, 1, oldRdr.closeCalls)
	require.Equal(t, 0, newRdr.closeCalls)
	require.Same(t, newRdr, reader.rdr)
	require.Nil(t, reader.curPage)

	require.NoError(t, reader.Close())
	require.Equal(t, 1, newRdr.closeCalls)
}

func TestColumnChunkReaderSetPageReaderPropagatesCloseError(t *testing.T) {
	closeErr := errors.New("page reader close failed")
	page := &closeTrackingPage{}
	oldRdr := &closeTrackingPageReader{closeErr: closeErr}
	newRdr := &closeTrackingPageReader{}
	reader := &columnChunkReader{curPage: page, rdr: oldRdr}

	require.ErrorIs(t, reader.setPageReader(newRdr), closeErr)
	require.ErrorIs(t, reader.err, closeErr)
	require.Equal(t, 1, page.releaseCalls)
	require.Equal(t, 1, oldRdr.closeCalls)
	require.Equal(t, 0, newRdr.closeCalls)
	require.Nil(t, reader.curPage)
	require.Nil(t, reader.rdr)
	require.NoError(t, reader.Close())
	require.Equal(t, 1, oldRdr.closeCalls)
}

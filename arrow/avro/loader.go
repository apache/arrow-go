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

package avro

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

func (r *OCFReader) decodeOCFToChan() {
	defer close(r.avroChan)
	for {
		select {
		case <-r.readerCtx.Done():
			r.setErr(fmt.Errorf("avro decoding cancelled, %d records read", r.avroDatumCount.Load()))
			return
		default:
			var datum any
			err := r.r.Decode(&datum)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				r.setErr(err)
				return
			}
			select {
			case r.avroChan <- datum:
				r.avroDatumCount.Add(1)
			case <-r.readerCtx.Done():
				r.setErr(fmt.Errorf("avro decoding cancelled, %d records read", r.avroDatumCount.Load()))
				return
			}
		}
	}
}

func (r *OCFReader) recordFactory() {
	defer close(r.recChan)
	defer close(r.bldDone)
	r.primed = true
	recChunk := 0
	switch {
	case r.chunk < 1:
		for data := range r.avroChan {
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.setErr(err)
				return
			}
		}
		r.sendRecord(r.bld.NewRecordBatch())
	case r.chunk >= 1:
		for data := range r.avroChan {
			if recChunk == 0 {
				r.bld.Reserve(r.chunk)
			}
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.setErr(err)
				return
			}
			recChunk++
			if recChunk >= r.chunk {
				if !r.sendRecord(r.bld.NewRecordBatch()) {
					return
				}
				recChunk = 0
			}
		}
		if recChunk != 0 {
			r.sendRecord(r.bld.NewRecordBatch())
		}
	}
}

func (r *OCFReader) sendRecord(rec arrow.RecordBatch) bool {
	select {
	case r.recChan <- rec:
		return true
	case <-r.readerCtx.Done():
		rec.Release()
		return false
	}
}

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

package flight_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/internal/arrdata"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type flightServer struct {
	mem memory.Allocator
	flight.BaseFlightServer
}

func (f *flightServer) getmem() memory.Allocator {
	if f.mem == nil {
		f.mem = memory.NewGoAllocator()
	}

	return f.mem
}

func (f *flightServer) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	expr := string(c.GetExpression())

	auth := ""
	authVal := flight.AuthFromContext(fs.Context())
	if authVal != nil {
		auth = authVal.(string)
	}

	for _, name := range arrdata.RecordNames {
		if expr != "" && expr != name {
			continue
		}

		recs := arrdata.Records[name]
		totalRows := int64(0)
		for _, r := range recs {
			totalRows += r.NumRows()
		}

		fs.Send(&flight.FlightInfo{
			Schema: flight.SerializeSchema(recs[0].Schema(), f.getmem()),
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{name, auth},
			},
			TotalRecords: totalRows,
			TotalBytes:   -1,
		})
	}

	return nil
}

func (f *flightServer) GetSchema(_ context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid flight descriptor")
	}

	recs, ok := arrdata.Records[in.Path[0]]
	if !ok {
		return nil, status.Error(codes.NotFound, "flight not found")
	}

	return &flight.SchemaResult{Schema: flight.SerializeSchema(recs[0].Schema(), f.getmem())}, nil
}

func (f *flightServer) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	recs, ok := arrdata.Records[string(tkt.GetTicket())]
	if !ok {
		return status.Error(codes.NotFound, "flight not found")
	}

	w := flight.NewRecordWriter(fs, ipc.WithSchema(recs[0].Schema()))
	for _, r := range recs {
		w.Write(r)
	}

	return nil
}

type servAuth struct{}

func (a *servAuth) Authenticate(c flight.AuthConn) error {
	tok, err := c.Read()
	if errors.Is(err, io.EOF) {
		return nil
	}

	if string(tok) != "foobar" {
		return errors.New("novalid")
	}

	if err != nil {
		return err
	}

	return c.Send([]byte("baz"))
}

func (a *servAuth) IsValid(token string) (interface{}, error) {
	if token == "baz" {
		return "bar", nil
	}
	return "", errors.New("novalid")
}

type ctxauth struct{}

type clientAuth struct{}

func (a *clientAuth) Authenticate(ctx context.Context, c flight.AuthConn) error {
	if err := c.Send(ctx.Value(ctxauth{}).([]byte)); err != nil {
		return err
	}

	_, err := c.Read()
	return err
}

func (a *clientAuth) GetToken(ctx context.Context) (string, error) {
	return ctx.Value(ctxauth{}).(string), nil
}

func TestListFlights(t *testing.T) {
	s := flight.NewFlightServer()
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	flightStream, err := client.ListFlights(context.Background(), &flight.Criteria{})
	if err != nil {
		t.Error(err)
	}

	for {
		info, err := flightStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Error(err)
		}

		fname := info.GetFlightDescriptor().GetPath()[0]
		recs, ok := arrdata.Records[fname]
		if !ok {
			t.Fatalf("got unknown flight info: %s", fname)
		}

		sc, err := flight.DeserializeSchema(info.GetSchema(), f.mem)
		if err != nil {
			t.Fatal(err)
		}

		if !recs[0].Schema().Equal(sc) {
			t.Fatalf("flight info schema transfer failed: \ngot = %#v\nwant = %#v\n", sc, recs[0].Schema())
		}

		var total int64 = 0
		for _, r := range recs {
			total += r.NumRows()
		}

		if info.TotalRecords != total {
			t.Fatalf("got wrong number of total records: got = %d, wanted = %d", info.TotalRecords, total)
		}
	}
}

func TestGetSchema(t *testing.T) {
	s := flight.NewFlightServer()
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	for name, testrecs := range arrdata.Records {
		t.Run("flight get schema: "+name, func(t *testing.T) {
			res, err := client.GetSchema(context.Background(), &flight.FlightDescriptor{Path: []string{name}})
			if err != nil {
				t.Fatal(err)
			}

			schema, err := flight.DeserializeSchema(res.GetSchema(), f.getmem())
			if err != nil {
				t.Fatal(err)
			}

			if !testrecs[0].Schema().Equal(schema) {
				t.Fatalf("schema not match: \ngot = %#v\nwant = %#v\n", schema, testrecs[0].Schema())
			}
		})
	}
}

func TestServer(t *testing.T) {
	f := &flightServer{}
	f.SetAuthHandler(&servAuth{})

	s := flight.NewFlightServer()
	s.Init("localhost:0")
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), &clientAuth{}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	err = client.Authenticate(context.WithValue(context.Background(), ctxauth{}, []byte("foobar")))
	if err != nil {
		t.Error(err)
	}

	ctx := context.WithValue(context.Background(), ctxauth{}, "baz")

	fistream, err := client.ListFlights(ctx, &flight.Criteria{Expression: []byte("decimal128")})
	if err != nil {
		t.Error(err)
	}

	fi, err := fistream.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if len(fi.FlightDescriptor.GetPath()) != 2 || fi.FlightDescriptor.GetPath()[1] != "bar" {
		t.Fatalf("path should have auth info: want %s got %s", "bar", fi.FlightDescriptor.GetPath()[1])
	}

	fdata, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte("decimal128")})
	if err != nil {
		t.Error(err)
	}

	r, err := flight.NewRecordReader(fdata)
	if err != nil {
		t.Error(err)
	}

	expected := arrdata.Records["decimal128"]
	idx := 0
	var numRows int64 = 0
	for {
		rec, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Error(err)
		}

		numRows += rec.NumRows()
		if !array.RecordEqual(expected[idx], rec) {
			t.Errorf("flight data stream records don't match: \ngot = %#v\nwant = %#v", rec, expected[idx])
		}
		idx++
	}

	if numRows != fi.TotalRecords {
		t.Fatalf("got %d, want %d", numRows, fi.TotalRecords)
	}
}

func TestServerWithAdditionalServices(t *testing.T) {
	f := &flightServer{}
	f.SetAuthHandler(&servAuth{})

	s := flight.NewFlightServer()
	s.Init("localhost:0")
	s.RegisterFlightService(f)

	// Enable health check.
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())

	// Enable reflection for grpcurl.
	reflection.Register(s)

	go s.Serve()
	defer s.Shutdown()

	// Flight client should not be affected by the additional services.
	flightClient, err := flight.NewFlightClient(s.Addr().String(), &clientAuth{}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Error(err)
	}
	defer flightClient.Close()

	// Make sure health check is working.
	conn, err := grpc.NewClient(s.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	healthClient := grpc_health_v1.NewHealthClient(conn)
	_, err = healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Error(err)
	}
}

type flightMetadataWriterServer struct {
	flight.BaseFlightServer
}

func (f *flightMetadataWriterServer) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	recs := arrdata.Records[string(tkt.GetTicket())]

	w := flight.NewRecordWriter(fs, ipc.WithSchema(recs[0].Schema()))
	defer w.Close()
	for idx, r := range recs {
		w.WriteWithAppMetadata(r, []byte(fmt.Sprintf("%d_%s", idx, string(tkt.GetTicket()))) /*metadata*/)
	}
	return nil
}

func TestFlightWithAppMetadata(t *testing.T) {
	f := &flightMetadataWriterServer{}
	s := flight.NewFlightServer()
	s.RegisterFlightService(f)
	s.Init("localhost:0")

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	fdata, err := client.DoGet(context.Background(), &flight.Ticket{Ticket: []byte("primitives")})
	if err != nil {
		t.Fatal(err)
	}

	r, err := flight.NewRecordReader(fdata)
	if err != nil {
		t.Fatal(err)
	}

	expected := arrdata.Records["primitives"]
	idx := 0
	for {
		rec, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}

		appMeta := r.LatestAppMetadata()
		if !array.RecordEqual(expected[idx], rec) {
			t.Errorf("flight data stream records for idx: %d don't match: \ngot = %#v\nwant = %#v", idx, rec, expected[idx])
		}

		exMeta := fmt.Sprintf("%d_primitives", idx)
		if string(appMeta) != exMeta {
			t.Errorf("flight data stream application metadata mismatch: got: %v, want: %v\n", string(appMeta), exMeta)
		}
		idx++
	}
}

type flightErrorReturn struct {
	flight.BaseFlightServer
}

func (f *flightErrorReturn) DoGet(_ *flight.Ticket, _ flight.FlightService_DoGetServer) error {
	return status.Error(codes.NotFound, "nofound")
}

func TestReaderError(t *testing.T) {
	f := &flightErrorReturn{}
	s := flight.NewFlightServer()
	s.RegisterFlightService(f)
	s.Init("localhost:0")

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	fdata, err := client.DoGet(context.Background(), &flight.Ticket{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = flight.NewRecordReader(fdata)
	if err == nil {
		t.Fatal("should have errored")
	}
}

func TestWriterInferSchema(t *testing.T) {
	recs, ok := arrdata.Records["primitives"]
	require.True(t, ok)

	fs := flightStreamWriter{}
	w := flight.NewRecordWriter(&fs)

	for _, rec := range recs {
		require.NoError(t, w.Write(rec))
	}

	require.NoError(t, w.Close())
}

func TestWriterInconsistentSchema(t *testing.T) {
	recs, ok := arrdata.Records["primitives"]
	require.True(t, ok)

	schema := arrow.NewSchema([]arrow.Field{{Name: "unknown", Type: arrow.PrimitiveTypes.Int8}}, nil)
	fs := flightStreamWriter{}
	w := flight.NewRecordWriter(&fs, ipc.WithSchema(schema))

	require.ErrorContains(t, w.Write(recs[0]), "arrow/ipc: tried to write record batch with different schema")
	require.NoError(t, w.Close())
}

type flightStreamWriter struct{}

// Send implements flight.DataStreamWriter.
func (f *flightStreamWriter) Send(data *flight.FlightData) error { return nil }

var _ flight.DataStreamWriter = (*flightStreamWriter)(nil)

// callbackRecordReader wraps a record reader and invokes a callback on each Next() call.
// It tracks whether batches are properly released and the reader itself is released.
type callbackRecordReader struct {
	mem            memory.Allocator
	schema         *arrow.Schema
	numBatches     int
	currentBatch   atomic.Int32
	onNext         func(batchIndex int) // callback invoked before returning from Next()
	released       atomic.Bool
	batchesCreated atomic.Int32
	totalRetains   atomic.Int32
	totalReleases  atomic.Int32
	createdBatches []arrow.RecordBatch // track all created batches for cleanup
	mu             sync.Mutex
}

func newCallbackRecordReader(mem memory.Allocator, schema *arrow.Schema, numBatches int, onNext func(int)) *callbackRecordReader {
	return &callbackRecordReader{
		mem:        mem,
		schema:     schema,
		numBatches: numBatches,
		onNext:     onNext,
	}
}

func (r *callbackRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *callbackRecordReader) Next() bool {
	current := r.currentBatch.Load()
	if int(current) >= r.numBatches {
		return false
	}
	r.currentBatch.Add(1)

	if r.onNext != nil {
		r.onNext(int(current))
	}

	return true
}

func (r *callbackRecordReader) RecordBatch() arrow.RecordBatch {
	bldr := array.NewInt64Builder(r.mem)
	defer bldr.Release()

	currentBatch := r.currentBatch.Load()
	bldr.AppendValues([]int64{int64(currentBatch)}, nil)
	arr := bldr.NewArray()

	rec := array.NewRecordBatch(r.schema, []arrow.Array{arr}, 1)
	arr.Release()

	tracked := &trackedRecordBatch{
		RecordBatch: rec,
		onRetain: func() {
			r.totalRetains.Add(1)
		},
		onRelease: func() {
			r.totalReleases.Add(1)
		},
	}

	r.mu.Lock()
	r.createdBatches = append(r.createdBatches, tracked)
	r.mu.Unlock()

	r.batchesCreated.Add(1)
	return tracked
}

func (r *callbackRecordReader) ReleaseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, batch := range r.createdBatches {
		batch.Release()
	}
	r.createdBatches = nil
}

func (r *callbackRecordReader) Retain() {}

func (r *callbackRecordReader) Release() {
	r.released.Store(true)
}

func (r *callbackRecordReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

func (r *callbackRecordReader) Err() error {
	return nil
}

// trackedRecordBatch wraps a RecordBatch to track Retain/Release calls.
type trackedRecordBatch struct {
	arrow.RecordBatch
	onRetain  func()
	onRelease func()
}

func (t *trackedRecordBatch) Retain() {
	if t.onRetain != nil {
		t.onRetain()
	}
	t.RecordBatch.Retain()
}

func (t *trackedRecordBatch) Release() {
	if t.onRelease != nil {
		t.onRelease()
	}
	t.RecordBatch.Release()
}

func TestStreamChunksFromReader_OK(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)

	rdr := newCallbackRecordReader(mem, schema, 5, nil)
	defer rdr.ReleaseAll()

	ch := make(chan flight.StreamChunk, 5)

	ctx := context.Background()

	go flight.StreamChunksFromReader(ctx, rdr, ch)

	var chunksReceived int
	for chunk := range ch {
		if chunk.Err != nil {
			t.Errorf("unexpected error chunk: %v", chunk.Err)
			continue
		}
		if chunk.Data != nil {
			chunksReceived++
			chunk.Data.Release()
		}
	}

	require.Equal(t, 5, chunksReceived, "should receive all 5 batches")
	require.True(t, rdr.released.Load(), "reader should be released")

}

// TestStreamChunksFromReader_HandlesCancellation verifies that context cancellation
// causes StreamChunksFromReader to exit cleanly and release the reader.
func TestStreamChunksFromReader_HandlesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)

	rdr := newCallbackRecordReader(mem, schema, 10, nil)
	defer rdr.ReleaseAll()
	ch := make(chan flight.StreamChunk) // unbuffered channel

	go flight.StreamChunksFromReader(ctx, rdr, ch)

	chunksReceived := 0
	for chunk := range ch {
		if chunk.Data != nil {
			chunksReceived++
			chunk.Data.Release()
		}

		// Cancel context after 2 batches (simulating server detecting client disconnect)
		if chunksReceived == 2 {
			cancel()
		}
	}

	// After canceling context, StreamChunksFromReader exits and closes the channel.
	// The for-range loop above exits when the channel closes.
	// By the time we reach here, the channel is closed, which means StreamChunksFromReader's
	// defer stack has already executed, so the reader must be released.

	require.True(t, rdr.released.Load(), "reader must be released when context is canceled")

}

// TestStreamChunksFromReader_CancellationReleasesBatches verifies that batches are
// properly tracked and demonstrates memory leaks without cleanup, then proves cleanup fixes it.
func TestStreamChunksFromReader_CancellationReleasesBatches(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create reader that will produce 10 batches, but we'll cancel after 3
	reader := newCallbackRecordReader(mem, schema, 10, func(batchIndex int) {
		if batchIndex == 2 {
			cancel()
		}
	})

	ch := make(chan flight.StreamChunk, 5)

	// Start streaming
	go flight.StreamChunksFromReader(ctx, reader, ch)

	// Consume chunks until channel closes
	var chunksReceived int
	for chunk := range ch {
		if chunk.Err != nil {
			t.Errorf("unexpected error chunk: %v", chunk.Err)
			continue
		}
		if chunk.Data != nil {
			chunksReceived++
			chunk.Data.Release()
		}
	}

	// Verify the reader was released
	require.True(t, reader.released.Load(), "reader should be released")

	// We should have received at most 3-4 chunks (depending on timing)
	// The important part is we didn't receive all 10
	require.LessOrEqual(t, chunksReceived, 4, "should not receive all 10 chunks, got %d", chunksReceived)
	require.Greater(t, chunksReceived, 0, "should receive at least 1 chunk")

	// Check that Retain and Release don't balance - proving there's a leak without manual cleanup
	retains := reader.totalRetains.Load()
	releases := reader.totalReleases.Load()
	batchesCreated := reader.batchesCreated.Load()

	// Each batch starts with refcount=1, then StreamChunksFromReader calls Retain() (refcount=2)
	// For sent batches: we call Release() (refcount=1), batch still has initial ref
	// For unsent batches due to cancellation: they keep refcount=1 from creation
	// So we expect: releases < retains + batchesCreated
	require.Less(t, releases, retains+batchesCreated,
		"without cleanup, releases should be less than retains+created: retains=%d, releases=%d, created=%d",
		retains, releases, batchesCreated)

	// Now manually release all created batches to show proper cleanup fixes the leak
	reader.ReleaseAll()

	// After cleanup, memory should be freed
	mem.AssertSize(t, 0)
}

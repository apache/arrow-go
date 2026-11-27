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
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// putServer counts the rows of every dataset it receives through DoPut.
type putServer struct {
	flight.BaseFlightServer
}

func (s *putServer) DoPut(stream flight.FlightService_DoPutServer) error {
	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	defer rdr.Release()

	// the descriptor only arrives with the first message of the stream
	path := strings.Join(rdr.LatestFlightDescriptor().GetPath(), "/")

	var rows int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	if rdr.Err() != nil {
		return rdr.Err()
	}

	return stream.Send(&flight.PutResult{
		AppMetadata: []byte(fmt.Sprintf("stored %d rows in %s", rows, path)),
	})
}

func Example_doPut() {
	server := flight.NewFlightServer()
	server.Init("localhost:0")
	server.RegisterFlightService(&putServer{})

	go server.Serve()
	defer server.Shutdown()

	client, err := flight.NewFlightClient(server.Addr().String(), nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	// DoPut is a bidirectional stream: the client sends record batches
	// and the server answers with a PutResult once it has read them all.
	stream, err := client.DoPut(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	wr.SetFlightDescriptor(&flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"example", "ids"},
	})

	for i := range int64(3) {
		bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{i * 10, i*10 + 1}, nil)
		rec := bldr.NewRecordBatch()
		err = wr.Write(rec)
		rec.Release()
		if err != nil {
			log.Fatal(err)
		}
	}

	// Close releases the writer. CloseSend ends the stream, which is what
	// makes the server's reader stop.
	if err = wr.Close(); err != nil {
		log.Fatal(err)
	}
	if err = stream.CloseSend(); err != nil {
		log.Fatal(err)
	}

	res, err := stream.Recv()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(res.AppMetadata))

	// the server is done after sending its result, so the stream ends
	if _, err = stream.Recv(); err != io.EOF {
		log.Fatal(err)
	}

	// Output:
	// stored 6 rows in example/ids
}

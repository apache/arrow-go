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
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// handshakeCookieFlightServer is a flight server that emits Set-Cookie
// response headers (and trailers) during Handshake, simulating a server
// that creates a session during the authentication flow (see GH-755).
type handshakeCookieFlightServer struct {
	flight.BaseFlightServer

	headerCookie     string // cookie attached via SendHeader during Handshake
	trailerCookie    string // cookie attached via SetTrailer during Handshake
	bearerToken      string // authorization header returned during Handshake
	sendPayload      bool   // if true, server sends a HandshakeResponse payload before closing
	mu               sync.Mutex
	lastIncomingCook []string // incoming Cookie header values observed on ListFlights
}

func (h *handshakeCookieFlightServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	md := metadata.MD{}
	if h.headerCookie != "" {
		md.Append("set-cookie", h.headerCookie)
	}
	if h.bearerToken != "" {
		md.Append("authorization", "Bearer "+h.bearerToken)
	}
	if len(md) > 0 {
		if err := stream.SendHeader(md); err != nil {
			return err
		}
	}

	if h.trailerCookie != "" {
		stream.SetTrailer(metadata.Pairs("set-cookie", h.trailerCookie))
	}

	if h.sendPayload {
		if err := stream.Send(&flight.HandshakeResponse{Payload: []byte("handshake-ok")}); err != nil {
			return err
		}
	}

	// Drain the client stream until it closes.
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (h *handshakeCookieFlightServer) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	h.mu.Lock()
	if md, ok := metadata.FromIncomingContext(fs.Context()); ok {
		h.lastIncomingCook = append([]string(nil), md.Get("cookie")...)
	} else {
		h.lastIncomingCook = nil
	}
	h.mu.Unlock()
	return nil
}

func (h *handshakeCookieFlightServer) observedCookies() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.lastIncomingCook...)
}

// TestHandshakeCookiePropagationViaAuthenticateBasicToken is a regression
// test for GH-755. It asserts that Set-Cookie headers returned by a
// Handshake/DoHandshake response are captured by the cookie middleware
// and attached to subsequent requests.
func TestHandshakeCookiePropagationViaAuthenticateBasicToken(t *testing.T) {
	srv := &handshakeCookieFlightServer{
		headerCookie: "session_id=sess_header_abc",
		bearerToken:  "my-bearer-token",
	}

	s := flight.NewServerWithMiddleware(nil)
	s.Init("localhost:0")
	s.RegisterFlightService(srv)

	go s.Serve()
	defer s.Shutdown()

	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(
		s.Addr().String(),
		nil,
		[]flight.ClientMiddleware{flight.NewClientCookieMiddleware()},
		creds,
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, err := client.AuthenticateBasicToken(context.Background(), "user", "pass")
	require.NoError(t, err)

	// Make a follow-up RPC. The cookie middleware must have captured
	// Set-Cookie from the Handshake response, and StartCall should
	// attach it as a Cookie header on this call.
	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	require.NoError(t, err)
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
	}

	cookies := srv.observedCookies()
	require.Len(t, cookies, 1, "expected exactly one Cookie header, got %v", cookies)
	assert.Contains(t, cookies[0], "session_id=sess_header_abc",
		"cookie middleware should propagate Set-Cookie from Handshake response headers")
}

// TestHandshakeCookiePropagationFromTrailers ensures cookies delivered as
// gRPC trailers (instead of initial metadata headers) are also captured
// by the cookie middleware during Handshake.
func TestHandshakeCookiePropagationFromTrailers(t *testing.T) {
	srv := &handshakeCookieFlightServer{
		trailerCookie: "session_id=sess_trailer_xyz",
		bearerToken:   "my-bearer-token",
	}

	s := flight.NewServerWithMiddleware(nil)
	s.Init("localhost:0")
	s.RegisterFlightService(srv)

	go s.Serve()
	defer s.Shutdown()

	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(
		s.Addr().String(),
		nil,
		[]flight.ClientMiddleware{flight.NewClientCookieMiddleware()},
		creds,
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, err := client.AuthenticateBasicToken(context.Background(), "user", "pass")
	require.NoError(t, err)

	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	require.NoError(t, err)
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
	}

	cookies := srv.observedCookies()
	require.Len(t, cookies, 1, "expected exactly one Cookie header, got %v", cookies)
	assert.Contains(t, cookies[0], "session_id=sess_trailer_xyz",
		"cookie middleware should propagate Set-Cookie from Handshake response trailers")
}

// TestHandshakeCookiePropagationWithServerPayload is the precise scenario
// reported in GH-755. The server attaches a Set-Cookie header AND sends
// back a HandshakeResponse payload. AuthenticateBasicToken only calls
// stream.Recv() once, which returns the payload (not io.EOF), so the
// streaming finishFn that would normally invoke HeadersReceived never
// fires. The cookie middleware must still capture the header cookie.
func TestHandshakeCookiePropagationWithServerPayload(t *testing.T) {
	srv := &handshakeCookieFlightServer{
		headerCookie: "session_id=sess_with_payload",
		bearerToken:  "my-bearer-token",
		sendPayload:  true,
	}

	s := flight.NewServerWithMiddleware(nil)
	s.Init("localhost:0")
	s.RegisterFlightService(srv)

	go s.Serve()
	defer s.Shutdown()

	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(
		s.Addr().String(),
		nil,
		[]flight.ClientMiddleware{flight.NewClientCookieMiddleware()},
		creds,
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, err := client.AuthenticateBasicToken(context.Background(), "user", "pass")
	require.NoError(t, err)

	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	require.NoError(t, err)
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
	}

	cookies := srv.observedCookies()
	require.Len(t, cookies, 1,
		"expected exactly one Cookie header, got %v (GH-755: cookie lost when Handshake returns a payload)", cookies)
	assert.Contains(t, cookies[0], "session_id=sess_with_payload")
}

// TestHandshakeCookieProcessedBeforeRecv verifies cookies are captured
// eagerly once stream.Header() returns successfully. This models the
// scenario where an application-level Handshake flow inspects response
// headers and makes further RPCs before draining the stream.
func TestHandshakeCookieProcessedBeforeRecv(t *testing.T) {
	srv := &handshakeCookieFlightServer{
		headerCookie: "session_id=eager_capture",
	}

	s := flight.NewServerWithMiddleware(nil)
	s.Init("localhost:0")
	s.RegisterFlightService(srv)

	go s.Serve()
	defer s.Shutdown()

	cookies := flight.NewCookieMiddleware()
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(
		s.Addr().String(),
		nil,
		[]flight.ClientMiddleware{flight.CreateClientMiddleware(cookies)},
		creds,
	)
	require.NoError(t, err)
	defer client.Close()

	// Drive the Handshake manually; inspect headers before calling Recv().
	authCtx := metadata.AppendToOutgoingContext(context.Background(),
		"Authorization", "Basic "+base64.RawStdEncoding.EncodeToString([]byte("user:pass")))

	stream, err := client.Handshake(authCtx)
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())

	hdr, err := stream.Header()
	require.NoError(t, err)
	require.Contains(t, strings.Join(hdr.Get("set-cookie"), ","), "eager_capture")

	// Clone the middleware while the original Handshake stream is still
	// open. If cookies were processed eagerly from the header, the clone
	// should already contain the session cookie.
	cloned := cookies.Clone()

	// Using the clone, make a unary-ish request against a second client
	// to observe the outgoing Cookie header.
	clientB, err := flight.NewClientWithMiddleware(
		s.Addr().String(),
		nil,
		[]flight.ClientMiddleware{flight.CreateClientMiddleware(cloned)},
		creds,
	)
	require.NoError(t, err)
	defer clientB.Close()

	ls, err := clientB.ListFlights(context.Background(), &flight.Criteria{})
	require.NoError(t, err)
	for {
		if _, err := ls.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
	}

	got := srv.observedCookies()
	require.Len(t, got, 1, "expected cloned middleware to send cookie from eagerly captured Handshake header, got %v", got)
	assert.Contains(t, got[0], "session_id=eager_capture")

	// Clean up original stream.
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
}

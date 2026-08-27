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

package driver

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRequestMetadataKeepsConfiguredAuthorization(t *testing.T) {
	credentials := grpcCredentials{
		token:  "trusted",
		params: map[string]string{"Authorization": "Bearer attacker", "Database": "analytics"},
	}

	metadata, err := credentials.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"authorization": "Bearer trusted",
		"database":      "analytics",
	}, metadata)
}

func TestRequestMetadataKeepsConfiguredAuthorizationOnTransport(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			authorization := metadata.ValueFromIncomingContext(ctx, "authorization")
			if len(authorization) != 1 || authorization[0] != "Bearer trusted" {
				return nil, status.Errorf(codes.Unauthenticated, "unexpected authorization metadata: %q", authorization)
			}
			return handler(ctx, req)
		},
	))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, listener.Close())
	})

	credentials := grpcCredentials{
		token:  "trusted",
		params: map[string]string{"Authorization": "Bearer attacker"},
	}
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(credentials),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := healthpb.NewHealthClient(conn)
	for i := 0; i < 100; i++ {
		_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
		require.NoErrorf(t, err, "request %d", i+1)
	}
}

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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package flight

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rejectingAuth struct{}

func (rejectingAuth) Authenticate(AuthConn) error { return nil }
func (rejectingAuth) IsValid(string) (interface{}, error) {
	return nil, errors.New("invalid token")
}

type rejectingAuthServer struct{ auth ServerAuthHandler }

func (s rejectingAuthServer) GetAuthHandler() ServerAuthHandler { return s.auth }

func TestServerAuthUnaryInterceptorInvalidToken(t *testing.T) {
	_, err := serverAuthUnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		Server: rejectingAuthServer{auth: rejectingAuth{}},
	}, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("handler called for an invalid token")
		return nil, nil
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("unexpected status code: got %v, want %v", got, want)
	}
}

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

package example

import (
	"context"
	"testing"
)

func TestCreateDBIsolation(t *testing.T) {
	db1, err := CreateDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	db2, err := CreateDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if _, err := db1.Exec("CREATE TABLE sentinel (value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db2.Exec("SELECT * FROM sentinel"); err == nil {
		t.Fatal("separate CreateDB calls unexpectedly shared state")
	}

	ctx := context.Background()
	conn1, err := db1.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	conn2, err := db1.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	if _, err := conn1.ExecContext(ctx, "INSERT INTO sentinel VALUES (42)"); err != nil {
		t.Fatal(err)
	}
	var value int
	if err := conn2.QueryRowContext(ctx, "SELECT value FROM sentinel").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 42 {
		t.Fatalf("unexpected sentinel value: got %d, want 42", value)
	}
}

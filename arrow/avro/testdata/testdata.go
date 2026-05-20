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

package testdata

import (
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

const (
	SchemaFileName     = "alltypes.avsc"
	sampleAvroFileName = "alltypes.avro"
	sampleJSONFileName = "alltypes.json"
	decimalTypeScale   = 2
)

type ByteArray []byte

func (b ByteArray) MarshalJSON() ([]byte, error) {
	return json.Marshal([]byte(b))
}

type TimestampJSON time.Time

func (t TimestampJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format(time.RFC3339Nano))
}

type TimeMillisJSON time.Duration

func (t TimeMillisJSON) MarshalJSON() ([]byte, error) {
	ts := time.Unix(0, int64(t)).UTC().Format("15:04:05.000")
	return json.Marshal(strings.TrimRight(ts, "0."))
}

type TimeMicrosJSON time.Duration

func (t TimeMicrosJSON) MarshalJSON() ([]byte, error) {
	ts := time.Unix(0, int64(t)).UTC().Format("15:04:05.000000")
	return json.Marshal(strings.TrimRight(ts, "0."))
}

type FixedJSON []byte

func (t FixedJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal([]byte(t))
}

type FixedUUIDJSON [16]byte

func (t FixedUUIDJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(uuid.UUID(t).String())
}

type DecimalJSON struct {
	Rat *big.Rat
}

func (t DecimalJSON) MarshalJSON() ([]byte, error) {
	num := new(big.Int).Set(t.Rat.Num())
	den := new(big.Int).Set(t.Rat.Denom())
	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(decimalTypeScale), nil)
	num.Mul(num, scaleFactor)
	num.Quo(num, den)
	s := fmt.Sprintf("%0*s", decimalTypeScale+1, num.String())
	point := len(s) - decimalTypeScale
	return json.Marshal(s[:point] + "." + s[point:])
}

type DurationJSON avro.Duration

func (t DurationJSON) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"months":      int32(t.Months),
		"days":        int32(t.Days),
		"nanoseconds": int64(t.Milliseconds) * int64(time.Millisecond),
	}
	return json.Marshal(m)
}

type DateJSON time.Time

func (t DateJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format("2006-01-02"))
}

type Example struct {
	InheritNull       string        `avro:"inheritNull"`
	ExplicitNamespace [12]byte      `avro:"explicitNamespace"`
	FullName          FullNameData  `avro:"fullName"`
	ID                int32         `avro:"id"`
	BigID             int64         `avro:"bigId"`
	Temperature       *float32      `avro:"temperature"`
	Fraction          *float64      `avro:"fraction"`
	IsEmergency       bool          `avro:"is_emergency"`
	RemoteIP          *[]byte       `avro:"remote_ip"`
	NullableRemoteIPS *[][]byte     `avro:"nullable_remote_ips"`
	Person            PersonData    `avro:"person"`
	DecimalField      *big.Rat      `avro:"decimalField"`
	Decimal256Field   *big.Rat      `avro:"decimal256Field"`
	UUIDField         string        `avro:"uuidField"`
	FixedUUIDField    [16]byte      `avro:"fixedUuidField"`
	TimeMillis        time.Duration `avro:"timemillis"`
	TimeMicros        time.Duration `avro:"timemicros"`
	TimestampMillis   time.Time     `avro:"timestampmillis"`
	TimestampMicros   time.Time     `avro:"timestampmicros"`
	LocalTSMillis     time.Time     `avro:"localtimestampmillis"`
	LocalTSMicros     time.Time     `avro:"localtimestampmicros"`
	Duration          avro.Duration `avro:"duration"`
	Date              time.Time     `avro:"date"`
}

func (e Example) MarshalJSON() ([]byte, error) {
	var remoteIP *ByteArray
	if e.RemoteIP != nil {
		v := ByteArray(*e.RemoteIP)
		remoteIP = &v
	}
	var nullableRemoteIPs *[]ByteArray
	if e.NullableRemoteIPS != nil {
		arr := make([]ByteArray, len(*e.NullableRemoteIPS))
		for i, b := range *e.NullableRemoteIPS {
			arr[i] = ByteArray(b)
		}
		nullableRemoteIPs = &arr
	}
	out := struct {
		InheritNull       string         `json:"inheritNull"`
		ExplicitNamespace FixedJSON      `json:"explicitNamespace"`
		FullName          fullNameJSON   `json:"fullName"`
		ID                int32          `json:"id"`
		BigID             int64          `json:"bigId"`
		Temperature       *float32       `json:"temperature"`
		Fraction          *float64       `json:"fraction"`
		IsEmergency       bool           `json:"is_emergency"`
		RemoteIP          *ByteArray     `json:"remote_ip"`
		NullableRemoteIPS *[]ByteArray   `json:"nullable_remote_ips"`
		Person            PersonData     `json:"person"`
		DecimalField      DecimalJSON    `json:"decimalField"`
		Decimal256Field   DecimalJSON    `json:"decimal256Field"`
		UUIDField         string         `json:"uuidField"`
		FixedUUIDField    FixedUUIDJSON  `json:"fixedUuidField"`
		TimeMillis        TimeMillisJSON `json:"timemillis"`
		TimeMicros        TimeMicrosJSON `json:"timemicros"`
		TimestampMillis   TimestampJSON  `json:"timestampmillis"`
		TimestampMicros   TimestampJSON  `json:"timestampmicros"`
		LocalTSMillis     TimestampJSON  `json:"localtimestampmillis"`
		LocalTSMicros     TimestampJSON  `json:"localtimestampmicros"`
		Duration          DurationJSON   `json:"duration"`
		Date              DateJSON       `json:"date"`
	}{
		InheritNull:       e.InheritNull,
		ExplicitNamespace: FixedJSON(e.ExplicitNamespace[:]),
		FullName:          fullNameJSON{InheritNamespace: e.FullName.InheritNamespace, Md5: FixedJSON(e.FullName.Md5[:])},
		ID:                e.ID,
		BigID:             e.BigID,
		Temperature:       e.Temperature,
		Fraction:          e.Fraction,
		IsEmergency:       e.IsEmergency,
		RemoteIP:          remoteIP,
		NullableRemoteIPS: nullableRemoteIPs,
		Person:            e.Person,
		DecimalField:      DecimalJSON{Rat: e.DecimalField},
		Decimal256Field:   DecimalJSON{Rat: e.Decimal256Field},
		UUIDField:         e.UUIDField,
		FixedUUIDField:    FixedUUIDJSON(e.FixedUUIDField),
		TimeMillis:        TimeMillisJSON(e.TimeMillis),
		TimeMicros:        TimeMicrosJSON(e.TimeMicros),
		TimestampMillis:   TimestampJSON(e.TimestampMillis),
		TimestampMicros:   TimestampJSON(e.TimestampMicros),
		LocalTSMillis:     TimestampJSON(e.LocalTSMillis),
		LocalTSMicros:     TimestampJSON(e.LocalTSMicros),
		Duration:          DurationJSON(e.Duration),
		Date:              DateJSON(e.Date),
	}
	return json.Marshal(out)
}

type FullNameData struct {
	InheritNamespace string   `avro:"inheritNamespace"`
	Md5              [16]byte `avro:"md5"`
}

type fullNameJSON struct {
	InheritNamespace string    `json:"inheritNamespace"`
	Md5              FixedJSON `json:"md5"`
}

type MapField map[string]int64

func (t MapField) MarshalJSON() ([]byte, error) {
	arr := make([]map[string]any, 0, len(t))
	for k, v := range t {
		arr = append(arr, map[string]any{"key": k, "value": v})
	}
	return json.Marshal(arr)
}

type PersonData struct {
	Lastname   string          `avro:"lastname" json:"lastname"`
	Address    AddressUSRecord `avro:"address" json:"address"`
	Mapfield   MapField        `avro:"mapfield" json:"mapfield"`
	ArrayField []string        `avro:"arrayField" json:"arrayField"`
}

type AddressUSRecord struct {
	Streetaddress string `avro:"streetaddress" json:"streetaddress"`
	City          string `avro:"city" json:"city"`
}

type TestPaths struct {
	Avro string
	Json string
}

func Generate() TestPaths {
	td, err := os.MkdirTemp("", "arrow-avro-testdata-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	data := sampleData()
	return TestPaths{
		Avro: writeOCFSampleData(td, data),
		Json: writeJSONSampleData(td, data),
	}
}

func TestdataDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get cwd: %v", err)
	}
	switch filepath.Base(cwd) {
	case "arrow-go":
		return filepath.Join("arrow", "avro", "testdata")
	case "avro":
		return "testdata"
	case "testdata":
		return "."
	}
	log.Fatalf("unexpected cwd: %s", cwd)
	return ""
}

// AllTypesAvroSchema returns the raw JSON of the bundled `alltypes.avsc`
// testdata schema.
func AllTypesAvroSchema() (string, error) {
	sp := filepath.Join(TestdataDir(), SchemaFileName)
	avroSchemaBytes, err := os.ReadFile(sp)
	if err != nil {
		return "", err
	}
	return string(avroSchemaBytes), nil
}

func sampleData() Example {
	now := time.Now().UTC()
	// Truncate to micros so timestamp-millis/-micros round-trip exactly.
	tsMillis := now.Truncate(time.Millisecond)
	tsMicros := now.Truncate(time.Microsecond)
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	decimal := new(big.Rat).SetFrac(big.NewInt(9876), big.NewInt(100)) // 98.76
	decimal256, ok := new(big.Rat).SetString("12345678901234567890123456789012345678901234567890123456.78")
	if !ok {
		log.Fatal("bad decimal256 literal in sampleData")
	}

	return Example{
		InheritNull:       "a",
		ExplicitNamespace: [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		FullName: FullNameData{
			InheritNamespace: "d",
			Md5:              [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
		ID:          42,
		BigID:       42000000000,
		Temperature: func() *float32 { v := float32(36.6); return &v }(),
		Fraction:    func() *float64 { v := float64(0.75); return &v }(),
		IsEmergency: true,
		RemoteIP:    func() *[]byte { v := []byte{192, 168, 1, 1}; return &v }(),
		Person: PersonData{
			Lastname: "Doe",
			Address: AddressUSRecord{
				Streetaddress: "123 Main St",
				City:          "Metropolis",
			},
			Mapfield:   MapField{"foo": 123},
			ArrayField: []string{"one", "two"},
		},
		DecimalField:    decimal,
		Decimal256Field: decimal256,
		UUIDField:       "123e4567-e89b-12d3-a456-426614174000",
		FixedUUIDField:  [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
		TimeMillis:      50412345 * time.Millisecond,
		TimeMicros:      50412345678 * time.Microsecond,
		TimestampMillis: tsMillis,
		TimestampMicros: tsMicros,
		LocalTSMillis:   tsMillis,
		LocalTSMicros:   tsMicros,
		Duration:        avro.Duration{Months: 1, Days: 2, Milliseconds: 3},
		Date:            date,
	}
}

func writeOCFSampleData(td string, data Example) string {
	path := filepath.Join(td, sampleAvroFileName)
	ocfFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer ocfFile.Close()
	schemaJSON, err := AllTypesAvroSchema()
	if err != nil {
		log.Fatal(err)
	}
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		log.Fatal(err)
	}
	// Pass the original JSON so logical-type annotations survive in the OCF header.
	encoder, err := ocf.NewWriter(ocfFile, schema, ocf.WithSchema(schemaJSON))
	if err != nil {
		log.Fatal(err)
	}
	defer encoder.Close()

	err = encoder.Encode(data)
	if err != nil {
		log.Fatal(err)
	}
	return path
}

func writeJSONSampleData(td string, data Example) string {
	path := filepath.Join(td, sampleJSONFileName)
	jsonFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer jsonFile.Close()
	enc := json.NewEncoder(jsonFile)
	err = enc.Encode(data)
	if err != nil {
		log.Fatal(err)
	}
	return path
}

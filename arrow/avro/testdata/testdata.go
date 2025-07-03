package testdata

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	avro "github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

const (
	SchemaFileName     = "alltypes.avsc"
	sampleAvroFileName = "alltypes.avro"
	sampleJSONFileName = "alltypes.json"
	decimalTypeScale   = 2
)

type ByteArray []byte

func (b ByteArray) MarshalJSON() ([]byte, error) {
	s := fmt.Sprint(b)
	encoded := base64.StdEncoding.EncodeToString([]byte(s))
	return json.Marshal(encoded)
}

type TimestampMicros int64

func (t TimestampMicros) MarshalJSON() ([]byte, error) {
	ts := time.Unix(0, int64(t)*int64(time.Microsecond)).UTC().Format("2006-01-02 15:04:05.000000")
	// arrow record marshaller trims trailing zero digits from timestamp so we do the same
	return json.Marshal(fmt.Sprintf("%sZ", strings.TrimRight(ts, "0.")))
}

type TimestampMillis int64

func (t TimestampMillis) MarshalJSON() ([]byte, error) {
	ts := time.Unix(0, int64(t)*int64(time.Millisecond)).UTC().Format("2006-01-02 15:04:05.000")
	return json.Marshal(fmt.Sprintf("%sZ", strings.TrimRight(ts, "0.")))
}

type TimeMillis time.Duration

func (t TimeMillis) MarshalJSON() ([]byte, error) {
	ts := time.Unix(0, int64(t)).UTC().Format("15:04:05.000")
	return json.Marshal(strings.TrimRight(ts, "0."))
}

type TimeMicros time.Duration

func (t TimeMicros) MarshalJSON() ([]byte, error) {
	ts := time.Unix(0, int64(t)).UTC().Format("15:04:05.000000")
	return json.Marshal(strings.TrimRight(ts, "0."))
}

type ExplicitNamespace [12]byte

func (t ExplicitNamespace) MarshalJSON() ([]byte, error) {
	return json.Marshal(t[:])
}

type MD5 [16]byte

func (t MD5) MarshalJSON() ([]byte, error) {
	return json.Marshal(t[:])
}

type DecimalType []byte

func (t DecimalType) MarshalJSON() ([]byte, error) {
	v := new(big.Int).SetBytes(t)
	s := fmt.Sprintf("%0*s", decimalTypeScale+1, v.String())
	point := len(s) - decimalTypeScale
	return json.Marshal(s[:point] + "." + s[point:])
}

type Duration [12]byte

func (t Duration) MarshalJSON() ([]byte, error) {
	milliseconds := int32(binary.LittleEndian.Uint32(t[8:12]))

	m := map[string]interface{}{
		"months":      int32(binary.LittleEndian.Uint32(t[0:4])),
		"days":        int32(binary.LittleEndian.Uint32(t[4:8])),
		"nanoseconds": int64(milliseconds) * int64(time.Millisecond),
	}
	return json.Marshal(m)
}

type Date int32

func (t Date) MarshalJSON() ([]byte, error) {
	v := time.Unix(int64(t)*86400, 0).UTC().Format("2006-01-02")
	return json.Marshal(v)
}

type Example struct {
	InheritNull       string            `avro:"inheritNull" json:"inheritNull"`
	ExplicitNamespace ExplicitNamespace `avro:"explicitNamespace" json:"explicitNamespace"`
	FullName          FullNameData      `avro:"fullName" json:"fullName"`
	ID                int32             `avro:"id" json:"id"`
	BigID             int64             `avro:"bigId" json:"bigId"`
	Temperature       *float32          `avro:"temperature" json:"temperature"`
	Fraction          *float64          `avro:"fraction" json:"fraction"`
	IsEmergency       bool              `avro:"is_emergency" json:"is_emergency"`
	RemoteIP          *ByteArray        `avro:"remote_ip" json:"remote_ip"`
	Person            PersonData        `avro:"person" json:"person"`
	DecimalField      DecimalType       `avro:"decimalField" json:"decimalField"`
	Decimal256Field   DecimalType       `avro:"decimal256Field" json:"decimal256Field"`
	UUIDField         string            `avro:"uuidField" json:"uuidField"`
	TimeMillis        TimeMillis        `avro:"timemillis" json:"timemillis"`
	TimeMicros        TimeMicros        `avro:"timemicros" json:"timemicros"`
	TimestampMillis   TimestampMillis   `avro:"timestampmillis" json:"timestampmillis"`
	TimestampMicros   TimestampMicros   `avro:"timestampmicros" json:"timestampmicros"`
	Duration          Duration          `avro:"duration" json:"duration"`
	Date              Date              `avro:"date" json:"date"`
}

type FullNameData struct {
	InheritNamespace string `avro:"inheritNamespace" json:"inheritNamespace"`
	Md5              MD5    `avro:"md5" json:"md5"`
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

func AllTypesAvroSchema() (avro.Schema, error) {
	sp := filepath.Join(TestdataDir(), SchemaFileName)
	avroSchemaBytes, err := os.ReadFile(sp)
	if err != nil {
		return nil, err
	}
	return avro.ParseBytes(avroSchemaBytes)
}

func sampleData() Example {
	return Example{
		InheritNull:       "a",
		ExplicitNamespace: ExplicitNamespace{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		FullName: FullNameData{
			InheritNamespace: "d",
			Md5:              MD5{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
		ID:          42,
		BigID:       42000000000,
		Temperature: func() *float32 { v := float32(36.6); return &v }(),
		Fraction:    func() *float64 { v := float64(0.75); return &v }(),
		IsEmergency: true,
		RemoteIP:    func() *ByteArray { v := ByteArray{192, 168, 1, 1}; return &v }(),
		Person: PersonData{
			Lastname: "Doe",
			Address: AddressUSRecord{
				Streetaddress: "123 Main St",
				City:          "Metropolis",
			},
			Mapfield:   MapField{"foo": 123},
			ArrayField: []string{"one", "two"},
		},
		DecimalField: DecimalType{0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x94},
		Decimal256Field: DecimalType{
			0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
			0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
			0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01,
		},
		UUIDField:       "123e4567-e89b-12d3-a456-426614174000",
		TimeMillis:      TimeMillis(50412345 * time.Millisecond),
		TimeMicros:      TimeMicros(50412345678 * time.Microsecond),
		TimestampMillis: TimestampMillis(time.Now().UnixNano() / int64(time.Millisecond)),
		TimestampMicros: TimestampMicros(time.Now().UnixNano() / int64(time.Microsecond)),
		Duration:        Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		Date:            Date(time.Now().Unix() / 86400),
	}
}

func writeOCFSampleData(td string, data Example) string {
	path := filepath.Join(td, sampleAvroFileName)
	ocfFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer ocfFile.Close()
	as := readAvroSchema(TestdataDir())
	encoder, err := ocf.NewEncoder(as.String(), ocfFile)
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

func readAvroSchema(td string) avro.Schema {
	avroSchemaBytes, err := os.ReadFile(filepath.Join(td, SchemaFileName))
	if err != nil {
		log.Fatal(err)
	}
	schema, err := avro.Parse(string(avroSchemaBytes))
	if err != nil {
		log.Fatal(err)
	}
	return schema
}

func writeJSONSampleData(td string, data Example) string {
	path := filepath.Join(td, sampleJSONFileName)
	jsonFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
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

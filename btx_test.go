package btx

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
)

func TestMarshalRow(t *testing.T) {
	_, err := MarshalRow()
	if err != nil {
		t.Fatalf("%s error: %v", t.Name(), err)
	}
	t.Logf("%s todo", t.Name())
}

func TestNewRowMutation(t *testing.T) {
	src := A{
		TString:  "hello",
		TBool:    false,
		TFloat32: 3.14,
		TRowKey:  "rk",
	}
	bmu, err := NewRowMutation(&src, time.Now())
	if err != nil {
		t.Fatalf("failed test with error: %v", err)
	}
	if bmu.Key != "rk" {
		t.Fatalf("failed to set row key")
	}
}

type A struct {
	TRowKey  string  `bigtable:"cf1:,rowkey"` // broken cf:col but has rowkey
	TBytes   []byte  `bigtable:"cf1:bytes"`
	TString  string  `bigtable:"cf1:string"`
	TBool    bool    `bigtable:"cf1:bool"`
	TInt     int     `bigtable:"cf1:int"`
	TInt8    int8    `bigtable:"cf1:int8"`
	TInt16   int16   `bigtable:"cf1:int16"`
	TInt32   int32   `bigtable:"cf1:int32"`
	TInt64   int64   `bigtable:"cf1:int64"`
	TUint    uint    `bigtable:"cf1:uint"`
	TUint8   uint8   `bigtable:"cf1:uint8"`
	TUint16  uint16  `bigtable:"cf1:uint16"`
	TUint32  uint32  `bigtable:"cf1:uint32"`
	TUint64  uint64  `bigtable:"cf1:uint64"`
	TFloat32 float32 `bigtable:"cf1:float32"`
	TFloat64 float64 `bigtable:"cf1:float64"`
}
type B struct {
	TString string `bigtable:"cf1:string"`
	TBool   bool   `bigtable:"cf1:bool"`
}
type C struct {
	TRowKey string `bigtable:",rowkey"`
	Ignore1 string `bigtable:"-"`
	Ignore2 bool   `bigtable:""`
	Empty   *B
}

// Target variables for scanning into.
// var (
// 	scanstr    string
// 	scanbytes  []byte
// 	scanint    int
// 	scanint8   int8
// 	scanint16  int16
// 	scanint32  int32
// 	scanint64  int64
// 	scanuint   uint
// 	scanuint8  uint8
// 	scanuint16 uint16
// 	scanuint32 uint32
// 	scanuint64 uint64
// 	scanf32    float32
// 	scanf64    float64
// 	scanbool   bool
// 	// scantime   time.Time
// )

// type conversionTest struct {
// 	s, d interface{} // source and dest
// 	// following are used if they're non-zero
// 	wantint    int64
// 	wantint8   int8
// 	wantint16  int16
// 	wantint32  int32
// 	wantint64  int64
// 	wantuint   uint64
// 	wantstr    string
// 	wantbytes  []byte
// 	wantuint32 uint32
// 	wantuint64 uint64
// 	wantf32    float32
// 	wantf64    float64
// 	wantbool   bool // used if d is of type *bool
// 	wanttime   time.Time
// }

// func newConversionTest() []conversionTest {
// 	// Return a fresh instance to test so "go test -count 2" works correctly.
// 	return []conversionTest{
// 		// Exact conversions (destination pointer type matches source type)
// 		{s: "foo", d: &scanstr, wantstr: "foo"},
// 		{s: 123, d: &scanint, wantint: 123},
// 		// {s: someTime, d: &scantime, wanttime: someTime},

// 		// To strings
// 		{s: "string", d: &scanstr, wantstr: "string"},
// 		{s: []byte("byteslice"), d: &scanstr, wantstr: "byteslice"},
// 		{s: 123, d: &scanstr, wantstr: "123"},
// 		{s: int8(123), d: &scanstr, wantstr: "123"},
// 		{s: int64(123), d: &scanstr, wantstr: "123"},
// 		{s: uint8(123), d: &scanstr, wantstr: "123"},
// 		{s: uint16(123), d: &scanstr, wantstr: "123"},
// 		{s: uint32(123), d: &scanstr, wantstr: "123"},
// 		{s: uint64(123), d: &scanstr, wantstr: "123"},
// 		{s: 1.5, d: &scanstr, wantstr: "1.5"},
// 	}
// }

func TestUnmarshalTypes(t *testing.T) {
	num := 42
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, int64(num))
	tInt := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, int64(num))
	tInt64 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, int8(num))
	tInt8 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, int16(num))
	tInt16 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, int32(num))
	tInt32 := buf.Bytes()

	// buf = &bytes.Buffer{}
	// _ = binary.Write(buf, binary.BigEndian, uint(num))
	// tUint := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, uint8(num))
	tUint8 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, uint16(num))
	tUint16 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, uint32(num))
	tUint32 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, uint64(num))
	tUint64 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, float32(num))
	tFloat32 := buf.Bytes()

	buf = &bytes.Buffer{}
	_ = binary.Write(buf, binary.BigEndian, float64(num))
	tFloat64 := buf.Bytes()

	type S struct {
		A A
		B B
		C C
	}
	tests := []struct {
		Name   string
		Row    bigtable.Row
		Assert func(s *S) bool
	}{
		{
			Name: "strings",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:string",
				Value:  []byte("hello"),
			}}},
			Assert: func(s *S) bool {
				return s.A.TString == "hello"
			},
		},
		{
			Name: "bytes",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:bytes",
				Value:  []byte("hello"),
			}}},
			Assert: func(s *S) bool {
				return string(s.A.TBytes) == string([]byte("hello"))
			},
		},
		{
			Name: "booleans",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:bool",
				Value:  []byte{byte(1)},
			}}},
			Assert: func(s *S) bool {
				return s.A.TBool == true
			},
		},

		{
			Name: "int",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:int",
				Value:  tInt,
			}}},
			Assert: func(s *S) bool {
				return s.A.TInt == 42
			},
		},

		{
			Name: "int8",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:int8",
				Value:  tInt8,
			}}},
			Assert: func(s *S) bool {
				return int(s.A.TInt8) == 42
			},
		},
		{
			Name: "int16",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:int16",
				Value:  tInt16,
			}}},
			Assert: func(s *S) bool {
				return int(s.A.TInt16) == 42
			},
		},
		{
			Name: "int32",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:int32",
				Value:  tInt32,
			}}},
			Assert: func(s *S) bool {
				return int(s.A.TInt32) == 42
			},
		},

		{
			Name: "int64",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:int64",
				Value:  tInt64,
			}}},
			Assert: func(s *S) bool {
				return s.A.TInt64 == 42
			},
		},

		{
			Name: "uint8",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:uint8",
				Value:  tUint8,
			}}},
			Assert: func(s *S) bool {
				return s.A.TUint8 == 42
			},
		},

		{
			Name: "uint16",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:uint16",
				Value:  tUint16,
			}}},
			Assert: func(s *S) bool {
				return s.A.TUint16 == 42
			},
		},

		{
			Name: "uint32",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:uint32",
				Value:  tUint32,
			}}},
			Assert: func(s *S) bool {
				return s.A.TUint32 == 42
			},
		},

		{
			Name: "uint64",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:uint64",
				Value:  tUint64,
			}}},
			Assert: func(s *S) bool {
				return s.A.TUint64 == 42
			},
		},

		{
			Name: "float32",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:float32",
				Value:  tFloat32,
			}}},
			Assert: func(s *S) bool {
				return s.A.TFloat32 == 42
			},
		},

		{
			Name: "float64",
			Row: bigtable.Row{"cf1": []bigtable.ReadItem{{
				Row:    "key1",
				Column: "cf1:float64",
				Value:  tFloat64,
			}}},
			Assert: func(s *S) bool {
				return s.A.TFloat64 == 42
			},
		},
	}

	for _, tt := range tests {
		s := &S{}
		if err := UnmarshalRow(tt.Row, s); err != nil {
			t.Fatalf("%s Failed with error: %v", tt.Name, err)
		}
		if !tt.Assert(s) {
			t.Fatalf("%s -  Failed to assert truth", tt.Name)
		}
	}
}

// TODO - corner cases where errors are returned.
func TestUnmarshalPathologies(t *testing.T) {

}

func TestUnmarshalRow(t *testing.T) {
	row := bigtable.Row{
		"cf1": []bigtable.ReadItem{
			{
				Row:    "key1",
				Column: "cf1:string",
				Value:  []byte("test"),
			},
		},
		"cf2": []bigtable.ReadItem{
			{
				Row:    "key1",
				Column: "cf1:bool",
				Value:  []byte{byte(1)},
			},
		},
	}

	type S struct {
		A A
		B B
		C C
	}
	var s S
	if err := UnmarshalRow(row, s); err == nil {
		t.Fatalf("did not fail when it should without pointer")
	}
	if err := UnmarshalRow(row, &s); err != nil {
		t.Fatalf("%s error: %v", t.Name(), err)
	}
	// t.Logf("Res>> %+v\n", s)
	if !s.A.TBool {
		t.Fatalf("failed to get bool true")
	}
	if s.B.TString != "test" {
		t.Fatalf("failed to get string")
	}
	if s.C.TRowKey != "key1" {
		t.Fatalf("failed to set row key")
	}
}

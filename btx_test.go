package btx

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
)

// A is a collection of types in column family 'cf1'
type A struct {
	TRowKey  string  `bigtable:",rowkey"` // broken cf:col but has rowkey
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

// B is a secondary column family 'cf2' to verify multiple CF.
type B struct {
	TString string `bigtable:"cf2:string"`
	TBool   bool   `bigtable:"cf2:bool"`
}

// C is another column family 'cf3' for more exotic use/testing.
type C struct {
	TStringMap map[string]string `bigtable:"cf3:$$"`
	Ignore1    string            `bigtable:"-"`
	Ignore2    bool              `bigtable:""`
	Empty      *B
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
	if s.A.TString != "test" {
		t.Fatalf("failed to get string")
	}
	if s.A.TRowKey != "key1" {
		t.Fatalf("failed to set row key")
	}
}

func TestStringMapMutation(t *testing.T) {
	c := C{
		TStringMap: map[string]string{
			"foo": "abc",
			"bar": "123",
		},
	}
	bmu, err := NewRowMutation(&c, time.Now())
	if err != nil {
		t.Fatalf("failed test with error: %v", err)
	}
	_ = bmu
}

func TestStringMapFromRow(t *testing.T) {
	row := bigtable.Row{
		"cf3": []bigtable.ReadItem{
			{
				Row:    "key1",
				Column: "cf3:x",
				Value:  []byte("X"),
			},
			{
				Row:    "key1",
				Column: "cf3:y",
				Value:  []byte("Y"),
			},
			{
				Row:    "key1",
				Column: "cf3:z",
				Value:  []byte("Z"),
			},
		},
	}
	type S struct {
		A A
		B B
		C C
	}
	var s S
	if err := UnmarshalRow(row, &s); err != nil {
		t.Fatalf("%s error: %v", t.Name(), err)
	}
	for c, v := range map[string]string{"x": "X", "y": "Y", "z": "Z"} {
		t.Logf("Map>> %+v\n", s.C.TStringMap)
		if cv := s.C.TStringMap[c]; cv != v {
			t.Fatalf("mapped key column c=%s failed to get v=%s; got %s", c, v, cv)
		}
	}
}

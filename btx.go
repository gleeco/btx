package btx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
)

const (
	// FieldTagName represents the struct tagging we bind values on
	// like `json:"user"`  we define `bigtable:"family:column"`
	// Yes the value is colon delimited and we must have both family and col name.
	FieldTagName = "bigtable"
	// use this option to map to the row key itself
	// ie.  `bigtable:",rowkey"`
	RowKeyOptionName = "rowkey"
	// The proper target value for a bigtable field is delimited as 'family:column'
	FamilyColumnDelimiter = ":"
)

// our intermediary mapping of family:column to *reflect.Value
type columnValueMap map[string][]*reflect.Value

func MarshalRow() ([]byte, error) {
	fmt.Printf("todo")
	return nil, nil
}

func UnmarshalRow(row bigtable.Row, dest interface{}) error {
	if row == nil {
		return nil
	}
	vx := reflect.ValueOf(dest)
	if vx.Kind() != reflect.Ptr {
		return fmt.Errorf("Invalid target interface - must be pointer")
	}
	mapTo := columnValueMap{}
	if err := mapRowStruct(vx, mapTo, ""); err != nil {
		return err
	}
	// fmt.Printf("mapTo >> %+v\n", mapTo)
	_, assignKey := mapTo[RowKeyOptionName]

	for _, ri := range row {
		for _, r := range ri {
			if mv, ok := mapTo[r.Column]; ok {
				if err := setValues(mv, r.Value); err != nil {
					return err
				}
			}
			// assignKey means we need to set. Every column has this property
			// we just want to bother with it 1x. NB. this is here and now row.Key() to make testable.
			if assignKey {
				if mv, ok := mapTo[RowKeyOptionName]; ok {
					if err := setValues(mv, []byte(r.Row)); err != nil {
						return err
					}
				}
				assignKey = false
			}
		}
	}
	return nil
}

type bigtableMutation struct {
	Key string
	Mut *bigtable.Mutation
}

func NewRowMutation(i interface{}, t time.Time) (*bigtableMutation, error) {
	vx := reflect.ValueOf(i)
	if vx.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("Invalid target interface - must be pointer")
	}
	mapTo := columnValueMap{}
	if err := mapRowStruct(vx, mapTo, ""); err != nil {
		return nil, err
	}
	m := bigtable.NewMutation()

	for k, v := range mapTo {
		// TODO - maybe having multi-struct using the same row value is insane and bad idea.
		if len(v) > 1 {
			return nil, fmt.Errorf("refuse to create mutation from poly key %s", k)
		}
		// fmt.Printf("converting value for %s\n", k)

		// if v[0].Interface() == nil {
		if v[0].IsZero() {
			// fmt.Printf("\tskipping empty %s\n", k)
			continue
		}
		cf := strings.Split(k, FamilyColumnDelimiter)
		if len(cf) != 2 {
			// fmt.Printf("ignoring map key %s", k)
			continue
		}
		b, err := getBytes(v[0])
		if err != nil {
			return nil, err
		}
		m.Set(cf[0], cf[1], bigtable.Time(t), b)
	}

	btm := &bigtableMutation{
		Mut: m,
	}
	// Set the key if already
	if v, ok := mapTo[RowKeyOptionName]; ok {
		b, err := getBytes(v[0])
		if err != nil {
			return nil, err
		}
		if len(b) > 0 {
			btm.Key = string(b)
		}
	}
	return btm, nil
}

func getBytes(v *reflect.Value) ([]byte, error) {
	var b *bytes.Buffer

	// fmt.Printf("getting bytes here %s>> %+v\n", v.Kind(), v.Interface())
	i := v.Interface()
	kind := v.Kind()
	switch kind {
	case reflect.Slice:
		if reflect.ValueOf(i).Type().Elem().Kind() == reflect.Uint8 {
			// []byte
			return (i).([]byte), nil
		}
	case reflect.String:
		return []byte((i).(string)), nil

	case reflect.Bool:
		// boolean is a single byte
		bv := 0
		if bo := (i).(bool); bo {
			bv = 1
		}
		return []byte{byte(bv)}, nil

	case reflect.Int16, reflect.Uint16:
		b = bytes.NewBuffer(make([]byte, 0, binary.MaxVarintLen16))

	case reflect.Int32, reflect.Uint32:
		b = bytes.NewBuffer(make([]byte, 0, binary.MaxVarintLen32))

	case reflect.Float32:
		b = bytes.NewBuffer(make([]byte, 0, binary.MaxVarintLen32))

	case reflect.Int64, reflect.Uint64, reflect.Int, reflect.Uint, reflect.Float64:
		b = bytes.NewBuffer(make([]byte, 0, binary.MaxVarintLen64))

	}
	if b != nil {
		switch v.Kind() {
		case reflect.Int, reflect.Int64:
			// i = int64(i.(int))
			i = (i).(int64)
		case reflect.Uint, reflect.Uint64:
			i = (i).(uint64)
		case reflect.Int16:
			i = (i).(int16)
		case reflect.Int32:
			i = (i).(int32)
		case reflect.Uint8:
			i = (i).(uint8)
		case reflect.Uint16:
			i = (i).(uint16)
		case reflect.Uint32:
			i = (i).(uint32)
		case reflect.Float32:
			i = (i).(float32)
		case reflect.Float64:
			i = (i).(float64)

		default:
			return nil, fmt.Errorf("unsupported number type %s", v.Kind().String())
		}
		err := binary.Write(b, binary.BigEndian, i)
		return b.Bytes(), err
	}
	return nil, fmt.Errorf("unsupported type: %v", v.Kind())
}

// Mutation but limited to a single family
// func NewFamilyRowMutation() {

// }

// setValues assigns column value to one or more reflect.Value pointers
// mapped to the specific family:column.
func setValues(values []*reflect.Value, colValue []byte) error {
	if colValue == nil {
		return nil
	}
	for _, v := range values {
		if err := setValue(v, colValue); err != nil {
			return err
		}
	}
	return nil
}

var typeOfBytes = reflect.TypeOf([]byte(nil))

func setValue(v *reflect.Value, cv []byte) error {
	// be sane .. or is just FUD?
	if !(v.IsValid() && v.CanSet()) {
		return fmt.Errorf("cannot set invalid value field")
	}

	switch v.Kind() {
	case reflect.Slice:
		if v.Type() != typeOfBytes {
			return fmt.Errorf("failed to handle slice")
		}
		v.SetBytes(cv)

	case reflect.String:
		v.SetString(string(cv))
	case reflect.Bool:
		// cv byte boolean is a single byte 0 or 1
		v.SetBool(cv[0] == 1)

	case reflect.Int, reflect.Int64:
		var n int64
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		// Necessary?
		if v.OverflowInt(n) {
			return fmt.Errorf("can't assign value due to %s-overflow", v.Kind())
		}
		v.SetInt(n)

	case reflect.Int8:
		var n int8
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(n))

	case reflect.Int16:
		var n int16
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(n))

	case reflect.Int32:
		var n int32
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(n))

	case reflect.Uint, reflect.Uint64:
		var n uint64
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.SetUint(n)

	case reflect.Uint8:
		var n uint8
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(n))

	case reflect.Uint16:
		var n uint16
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(n))

	case reflect.Uint32:
		var n uint32
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(n))

	case reflect.Float32:
		var n float32
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(n))

	case reflect.Float64:
		var n float64
		if err := binary.Read(bytes.NewReader(cv), binary.BigEndian, &n); err != nil {
			return err
		}
		v.SetFloat(n)

	default:
		return fmt.Errorf("unsupported type. %v", v.Kind())
	}
	return nil
}

// fq: family qualifier
func mapRowStruct(val reflect.Value, mapTo map[string][]*reflect.Value, fq string) error {
	// TODO - this may (not) be a thing. Early hacking was plenty of confusion...
	// if val.Kind() == reflect.Interface && !val.IsNil() {
	// 	elm := val.Elem()
	// 	if elm.Kind() == reflect.Ptr && !elm.IsNil() && elm.Elem().Kind() == reflect.Ptr {
	// 		val = elm
	// 	}
	// }
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		kind := valueField.Kind()
		if kind == reflect.Struct {
			if err := mapRowStruct(valueField, mapTo, fq); err != nil {
				return err
			}
			continue
		}

		tagName, tagOpt := parseFieldTag(typeField.Tag.Get(FieldTagName))
		if tagOpt.Has(RowKeyOptionName) {
			tagName = RowKeyOptionName
		} else {
			if tagName == "" || tagName == "-" {
				continue
			}
			if fq != "" && strings.Split(tagName, FamilyColumnDelimiter)[0] != fq {
				continue
			}
		}
		mapTo[tagName] = append(mapTo[tagName], &valueField)
	}
	return nil
}

// Field Tag handling from fatih/structs

type tagOptions []string

func parseFieldTag(tag string) (string, tagOptions) {
	// tag is one of followings:
	// ""
	// "name"
	// "name,opt"
	// "name,opt,opt2"
	// ",opt"
	res := strings.Split(tag, ",")
	return res[0], res[1:]
}

// Has returns true if the given option is available in tagOptions
func (t tagOptions) Has(opt string) bool {
	for _, tagOpt := range t {
		if tagOpt == opt {
			return true
		}
	}
	return false
}

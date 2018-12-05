/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package tablestore

import (
	"strconv"
	"time"
	"github.com/mad-day/hobbydb/lstore"
	"github.com/vmihailenco/msgpack"
	uuid "github.com/nu7hatch/gouuid"
	"gopkg.in/src-d/go-vitess.v1/sqltypes"
	"gopkg.in/src-d/go-vitess.v1/vt/proto/query"
)

const tm_type     = 0xF
const tf_unsigned = 0x10

const (
	T_null = iota
	T_i8
	T_i16
	T_i24
	T_i32
	T_i64
	T_f32
	T_f64
	T_timestamp
	T_date
	T_text // aka varchar
	T_boolean
	T_json
	T_blob // aka varbinary
	
	// Unsigned variants
	T_u8  = T_i8  | tf_unsigned
	T_u16 = T_i16 | tf_unsigned
	T_u24 = T_i24 | tf_unsigned
	T_u32 = T_i32 | tf_unsigned
	T_u64 = T_i64 | tf_unsigned
)

// A representation of a Type identifier.
type T uint
func (u T) String() string {
	s := ""
	if (u&tf_unsigned)!=0 { s = "u_" }
	switch u&tm_type {
	case T_null: return "null"
	case T_i8 : return s+"int_8"
	case T_i16: return s+"int_16"
	case T_i24: return s+"int_24"
	case T_i32: return s+"int_32"
	case T_i64: return s+"int_64"
	case T_f32: return "float"
	case T_f64: return "double"
	case T_timestamp: return "timestamp"
	case T_date: return "date"
	case T_text: return "text"
	case T_boolean: return "bit"
	case T_json: return "json"
	case T_blob: return "blob"
	}
	return "undefined"
}
func (u T) Type() query.Type {
	switch u {
	case T_null: return sqltypes.Null
	case T_i8: return sqltypes.Int8
	case T_i16: return sqltypes.Int16
	case T_i24: return sqltypes.Int24
	case T_i32: return sqltypes.Int32
	case T_i64: return sqltypes.Int64
	case T_f32: return sqltypes.Float32
	case T_f64: return sqltypes.Float64
	
	case T_timestamp: return sqltypes.Timestamp
	case T_date: return sqltypes.Date
	case T_text: return sqltypes.Text
	case T_boolean: return sqltypes.Bit
	
	case T_json: return sqltypes.TypeJSON
	case T_blob: return sqltypes.Blob
	
	case T_u8: return sqltypes.Uint8
	case T_u16: return sqltypes.Uint16
	case T_u24: return sqltypes.Uint24
	case T_u32: return sqltypes.Uint32
	case T_u64: return sqltypes.Uint64
	}
	
	// Undefined!
	return sqltypes.Null
}

func (u T) bits() (i int) {
	switch u {
	case T_f32: i = 32
	case T_f64: i = 64
	}
	return
}

func (u T) ValueFrom(i interface{}) sqltypes.Value {
	switch u {
	case T_i8,T_i16,T_i24,T_i32,T_i64:
		return sqltypes.MakeTrusted(u.Type(),strconv.AppendInt(nil, i.(int64), 10))
	case T_u8,T_u16,T_u24,T_u32,T_u64:
		return sqltypes.MakeTrusted(u.Type(),strconv.AppendUint(nil, i.(uint64), 10))
	case T_f32:
		return sqltypes.MakeTrusted(u.Type(),strconv.AppendFloat(nil, float64(i.(float32)), 'g', -1, 32))
	case T_f64:
		return sqltypes.MakeTrusted(u.Type(),strconv.AppendFloat(nil, i.(float64), 'g', -1, 64))
	case T_timestamp:
		return sqltypes.MakeTrusted(u.Type(),i.(time.Time).AppendFormat(nil, "2006-01-02 15:04:05"))
	case T_date:
		return sqltypes.MakeTrusted(u.Type(),i.(time.Time).AppendFormat(nil, "2006-01-02"))
	case T_text,T_json,T_blob:
		return sqltypes.MakeTrusted(u.Type(),i.([]byte))
	case T_boolean:
		b := []byte{'0'}
		if i.(bool) { b[0] = '1' }
		return sqltypes.MakeTrusted(u.Type(),b)
	}
	return sqltypes.MakeTrusted(u.Type(),[]byte{})
}
func (u T) Encode(enc *msgpack.Encoder,i interface{}) error {
	switch u {
	case T_i8,T_i16,T_i24,T_i32,T_i64:
		return enc.EncodeInt(i.(int64))
	case T_u8,T_u16,T_u24,T_u32,T_u64:
		return enc.EncodeUint(i.(uint64))
	case T_f32:
		return enc.EncodeFloat32(i.(float32))
	case T_f64:
		return enc.EncodeFloat64(i.(float64))
	case T_timestamp,T_date:
		return enc.EncodeTime(i.(time.Time))
	case T_text,T_json,T_blob:
		return enc.EncodeBytes(i.([]byte))
	case T_boolean:
		return enc.EncodeBool(i.(bool))
	}
	return nil
}
func (u T) Decode(dec *msgpack.Decoder) (i interface{},e error) {
	switch u {
	case T_i8,T_i16,T_i24,T_i32,T_i64:
		i,e = dec.DecodeInt64()
	case T_u8,T_u16,T_u24,T_u32,T_u64:
		i,e = dec.DecodeUint64()
	case T_f32:
		i,e = dec.DecodeFloat32()
	case T_f64:
		i,e = dec.DecodeFloat64()
	case T_timestamp,T_date:
		i,e = dec.DecodeTime()
	case T_text,T_json,T_blob:
		i,e = dec.DecodeBytes()
	case T_boolean:
		i,e = dec.DecodeBool()
	}
	return
}

type tableColumn struct{
	_msgpack struct{} `msgpack:",asArray"`
	Name string
	Type uint
}

type tableSpec struct{
	Name string
	Guid *uuid.UUID
	Columns []tableColumn
	Pk int
}

type CatalogTx struct{
	UDB lstore.UDB
}
func (c *CatalogTx) getTable(sch,name string) (err error){
	c.UDB.UTable("catalog-tables")
	panic("...")
}

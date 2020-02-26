/*
Copyright (c) 2019 Simon Schmidt

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


package msgp

import "bytes"
import . "github.com/vmihailenco/msgpack"
import "github.com/vmihailenco/msgpack"
import "github.com/src-d/go-mysql-server/sql"

func MarshalMulti(v ...interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := NewEncoder(&buf).UseCompactEncoding(true).EncodeMulti(v...)
	return buf.Bytes(), err
}
func UnmarshalMulti(data []byte, v ...interface{}) error {
	return NewDecoder(bytes.NewReader(data)).UseDecodeInterfaceLoose(true).DecodeMulti(v...)
}

func Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := NewEncoder(&buf).UseCompactEncoding(true).Encode(v)
	return buf.Bytes(), err
}
func Unmarshal(data []byte, v interface{}) error {
	return NewDecoder(bytes.NewReader(data)).UseDecodeInterfaceLoose(true).Decode(v)
}


type Entity struct{
	record map[string]interface{}
}


// Implement msgpack.CustomDecoder
func (e *Entity) DecodeMsgpack(dec *msgpack.Decoder) error {
	return dec.Decode(e.record)
}
var _ msgpack.CustomDecoder = (*Entity)(nil)

// Implement msgpack.CustomEncoder
func (e *Entity) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(e.record)
}
var _ msgpack.CustomEncoder = (*Entity)(nil)


func (e *Entity) Init() {
	e.record = make(map[string]interface{})
}
func (e *Entity) UnmarshalOrInit(data []byte) error {
	err := Unmarshal(data,&e.record)
	if e.record==nil { e.record = make(map[string]interface{}) }
	return err
}
func (e *Entity) Unmarshal(data []byte) error {
	return Unmarshal(data,&e.record)
}
func (e *Entity) Marshal() ([]byte,error) {
	return Marshal(e.record)
}
func (e *Entity) GetValue(name string) (v interface{},ok bool) {
	v,ok = e.record[name]
	return
}
func (e *Entity) GetRow(sch sql.Schema) (row sql.Row,err error) {
	row = make(sql.Row,len(sch))
	for i,col := range sch {
		var ok bool
		row[i],ok = e.record[col.Name]
		if !ok { row[i] = col.Default }
		row[i],err = col.Type.Convert(row[i])
		if err!=nil { return }
	}
	return
}
func (e *Entity) SetRow(sch sql.Schema,row sql.Row) {
	for i,col := range sch {
		e.record[col.Name] = row[i]
	}
	return
}


// ##

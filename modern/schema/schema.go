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


/*
Table schema description format.
*/
package schema

import (
	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"strconv"
	"strings"
	"fmt"
	"regexp"
)

type Type uint
const (
	Null Type = iota
	Int8
	Uint8
	Int16
	Uint16
	Int32
	Int64
	Uint32
	Uint64
	Float32
	Float64
	
	Timestamp
	Date
	Text
	Boolean
	JSON
	Blob
	
	t_max
)


func (t Type) Compatible(v Type) bool {
	if t==Null { return v==Null }
	if t<=Float64 { return v>Null && v<=Float64 }
	if t<=Date { return t>Float64 && v<Date }
	return t==v
}

var tsqlv = [...]sql.Type {
	sql.Null,
	sql.Int8,
	sql.Uint8,
	sql.Int16,
	sql.Uint16,
	sql.Int32,
	sql.Int64,
	sql.Uint32,
	sql.Uint64,
	sql.Float32,
	sql.Float64,
	
	sql.Timestamp,
	sql.Date,
	sql.Text,
	sql.Boolean,
	sql.JSON,
	sql.Blob,
}
var tqueryv = [...]query.Type {
	sqltypes.Null,
	sqltypes.Int8,
	sqltypes.Uint8,
	sqltypes.Int16,
	sqltypes.Uint16,
	sqltypes.Int32,
	sqltypes.Int64,
	sqltypes.Uint32,
	sqltypes.Uint64,
	sqltypes.Float32,
	sqltypes.Float64,
	
	sqltypes.Timestamp,
	sqltypes.Date,
	sqltypes.Text,
	sqltypes.Bit,
	sqltypes.TypeJSON,
	sqltypes.Blob,
}

func FromSqlType(st sql.Type) (Type, bool) {
	for i,t := range tsqlv {
		if t==st { return Type(i),true }
	}
	return 0,false
}
func FromMysqlType(sql query.Type) (Type, bool) {
	switch sql {
	case sqltypes.Null:
		return Null, true
	case sqltypes.Int8:
		return Int8, true
	case sqltypes.Uint8:
		return Uint8, true
	case sqltypes.Int16:
		return Int16, true
	case sqltypes.Uint16:
		return Uint16, true
	case sqltypes.Int32:
		return Int32, true
	case sqltypes.Int64:
		return Int64, true
	case sqltypes.Uint32:
		return Uint32, true
	case sqltypes.Uint64:
		return Uint64, true
	case sqltypes.Float32:
		return Float32, true
	case sqltypes.Float64:
		return Float64, true
	case sqltypes.Timestamp:
		return Timestamp, true
	case sqltypes.Date:
		return Date, true
	case sqltypes.Text, sqltypes.VarChar:
		return Text, true
	case sqltypes.Bit:
		return Boolean, true
	case sqltypes.TypeJSON:
		return JSON, true
	case sqltypes.Blob:
		return Blob, true
	default:
		return 0, false
	}
}

func (t Type) String() string {
	if t>t_max { return "???" }
	return tsqlv[t].String()
}

func (t Type) Valid() bool { return t>t_max }
func (t Type) SqlType() sql.Type { return tsqlv[t] }
func (t Type) Type() query.Type { return tqueryv[t] }


type TableColumn struct{
	Name     string
	Type     Type
	Default  interface{}
	Nullable bool
}
func (t TableColumn) ToSqlColumn(tab string) *sql.Column {
	tp := t.Type.SqlType()
	v,_ := tp.Convert(t.Default)
	return &sql.Column{
		Name     : t.Name,
		Type     : tp,
		Default  : v,
		Nullable : t.Nullable,
		Source   : tab,
	}
}
func (t TableColumn) String() string {
	d := ""
	n := ""
	switch {
	case t.Default!=nil:
		v,e := t.Type.SqlType().SQL(t.Default)
		if e!=nil { break }
		r,e := sqlparser.ExprFromValue(v)
		if e!=nil { break }
		d = " default "+sqlparser.String(r)
	}
	if !t.Nullable { n = " not null" }
	return fmt.Sprint(t.Name," ",t.Type,d,n)
}

type TableSchema []TableColumn
func (t TableSchema) Find(col string) (TableColumn,bool) {
	for _,c := range t {
		if c.Name!=col { continue }
		return c,true
	}
	return TableColumn{},false
}
func (t TableSchema) FindIndex(col string) (int,bool) {
	for i,c := range t {
		if c.Name!=col { continue }
		return i,true
	}
	return -1,false
}
func (t TableSchema) Has(col string) (bool) {
	for i := range t {
		if t[i].Name!=col { continue }
		return true
	}
	return false
}
func (t TableSchema) ToSqlSchema(tab string) sql.Schema {
	s := make(sql.Schema,len(t))
	for i := range t {
		s[i] = t[i].ToSqlColumn(tab)
	}
	return s
}
func (t TableSchema) String() string {
	bdr := new(strings.Builder)
	bdr.WriteString("(")
	for i,v := range t {
		if i!=0 { bdr.WriteString(", ") }
		bdr.WriteString(v.String())
	}
	bdr.WriteString(")")
	return bdr.String()
}

func parsehexint(s string) (interface{},error){
	s = strings.ToLower(s)
	if strings.HasPrefix(s,"0x") {
		s = s[2:]
	} else if strings.HasPrefix(s,"x") {
		s = strings.Trim(s[1:], "'")
	}
	return strconv.ParseFloat(s, 64)
}

func extract(e sqlparser.Expr,t Type) (val interface{},err error) {
	for{
		switch v := e.(type) {
		case nil: return
		case *sqlparser.ParenExpr:
			e = v.Expr
		case *sqlparser.SQLVal:
			// v.Val
			switch v.Type {
			case sqlparser.StrVal:
				val = string(v.Val)
			case sqlparser.IntVal:
				val, err = strconv.ParseInt(string(v.Val), 10, 64)
			case sqlparser.FloatVal:
				val, err = strconv.ParseFloat(string(v.Val), 64)
			case sqlparser.HexNum:
				val, err = parsehexint(string(v.Val))
			case sqlparser.HexVal:
				val, err = v.HexDecode()
			case sqlparser.ValArg:
				val, err = nil,fmt.Errorf("'?' or ':vxyz' placeholders are not supported in DDL")
			case sqlparser.BitVal:
				if len(v.Val)<1 {
					err = fmt.Errorf("illegal bit: %q",v.Val)
				} else {
					val = v.Val[0]=='1'
				}
			default: return nil,fmt.Errorf("Unknown SQL Literal type %v",v.Type)
			}
			if err==nil { val,err = t.SqlType().Convert(val) }
			return
		default:
			return nil,fmt.Errorf("Unknown SQL expr %v",sqlparser.String(e))
		}
	}
}

func FromTablespec(sp *sqlparser.TableSpec) (ts TableSchema,err error) {
	ts = make(TableSchema,len(sp.Columns))
	for i,cd := range sp.Columns {
		col := cd.Type
		tp,ok := FromMysqlType(col.SQLType())
		if !ok { err = fmt.Errorf("Unsupported type: %s",col.Type); return }
		var def interface{}
		def,err = extract(col.Default,tp)
		if err!=nil { return }
		ts[i] = TableColumn{
			Name: cd.Name.String(),
			Type: tp,
			Default: def,
			Nullable: !bool(col.NotNull),
		}
	}
	return
}

var ct_from = regexp.MustCompile(`from\s+([a-zA-Z0-9_]+)`)

type TableMetadata struct{
	Name   string
	Schema TableSchema
	
	From   string
}
func (t *TableMetadata) String() string {
	return fmt.Sprintf("create table %s %v from %s",t.Name,t.Schema,t.From)
}
func (t *TableMetadata) SqlSchema() sql.Schema {
	return t.Schema.ToSqlSchema(t.Name)
}

func ParseTableMetadata(s *sqlparser.DDL) (md *TableMetadata,err error) {
	if s.Action!=sqlparser.CreateStr { return nil,fmt.Errorf("Expected 'create', got '%s'",s.Action) }
	
	md = new(TableMetadata)
	
	md.Name = s.Table.Name.String()
	md.From = md.Name
	
	md.Schema,err = FromTablespec(s.TableSpec)
	if err!=nil { return }
	
	if sm := ct_from.FindStringSubmatch(s.TableSpec.Options); len(sm)!=0 {
		md.From = sm[1]
	}
	
	return
}

type TableMasterMetadata struct{
	Name     string
	Schema   TableSchema
	PKey     []string
	Index    [][]string
	Unique   [][]string
	Options  string
}
func (t *TableMasterMetadata) String() string {
	return fmt.Sprintf("create master-table %s %v primary key %v indeces %v unique %v options%v",t.Name,t.Schema,t.PKey,t.Index,t.Unique,t.Options)
}

func ParseTableMasterMetadata(s *sqlparser.DDL) (md *TableMasterMetadata,err error) {
	if s.Action!=sqlparser.CreateStr { return nil,fmt.Errorf("Expected 'create', got '%s'",s.Action) }
	
	md = new(TableMasterMetadata)
	
	md.Name = s.Table.Name.String()
	
	md.Schema,err = FromTablespec(s.TableSpec)
	if err!=nil { return }
	
	for _,idx := range s.TableSpec.Indexes {
		fmt.Println("[]",idx.Info.Type)
		switch idx.Info.Type {
		case "primary key":
			for _,col := range idx.Columns {
				md.PKey = append(md.PKey,col.Column.String())
			}
		case "key","unique", "unique key":
			n := make([]string,len(idx.Columns))
			for i,col := range idx.Columns {
				n[i] = col.Column.String()
			}
			switch idx.Info.Type {
			case "key":
				md.Index = append(md.Index,n)
			case "unique","unique key":
				md.Unique = append(md.Unique,n)
			}
		}
	}
	
	md.Options = s.TableSpec.Options
	
	return
}
func (t *TableMasterMetadata) CompatibleWith(sch *TableMetadata) (compatible bool) {
	compatible = true
	
	for _,col := range sch.Schema {
		if n,ok := t.Schema.Find(col.Name); ok {
			if !n.Type.Compatible(col.Type) { compatible = false }
		}
	}
	for _,pk := range t.PKey {
		if _,ok := sch.Schema.Find(pk); !ok { compatible = false }
	}
	for _,elem := range t.Index {
		for _,e := range elem {
			if _,ok := sch.Schema.Find(e); !ok { compatible = false }
		}
	}
	for _,elem := range t.Unique {
		for _,e := range elem {
			if _,ok := sch.Schema.Find(e); !ok { compatible = false }
		}
	}
	
	return
}

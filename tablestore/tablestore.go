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



package tablestore

import (
	"github.com/mad-day/hobbydb/lstore"
	"github.com/mad-day/hobbydb/modern/schema"
	"github.com/mad-day/hobbydb/modern/containers"
	"github.com/mad-day/hobbydb/modern/msgp"
	"github.com/mad-day/hobbydb/modern/preprocess"
	//"github.com/mad-day/hobbydb/modern/eplan"
	"github.com/src-d/go-mysql-server/sql"
	
	"io"
	"errors"
	"fmt"
)

var (
	EBadRowFormat = errors.New("Bad Row Format")
	EBadKeyFormat = errors.New("Bad Key Format")

	EDuplicateKey = errors.New("Duplicate Key")
	
	ENullConstraint = errors.New("Null Constraint Error")
)

type closeable struct{}
func (closeable) Close() error { return nil }

type TableMetadata struct {
	Master     schema.TableMasterMetadata
	Format     string
	PrimaryKey []int
}
func (tmd *TableMetadata) StoreInto(udb lstore.UDB) error {
	db,err := udb.UTable("catalog-tables")
	if err!=nil { return err }
	
	val,err := msgp.Marshal(tmd)
	if err!=nil { return err }
	
	return db.Write([]byte(tmd.Master.Name),val)
}


type SimpleTable struct{
	UDB        lstore.UDB
	ut         lstore.UTable
	InnerName  string
	ItsName    string
	ItsSchema  sql.Schema
	PrimaryKey int
	PKName     string
}
func (s *SimpleTable) Name() string { return s.ItsName }
func (s *SimpleTable) String() string {
	return "SimpleTable{"+s.ItsName+"}"
}
func (s *SimpleTable) Schema() sql.Schema { return s.ItsSchema }
func (s *SimpleTable) Partitions(c *sql.Context) (sql.PartitionIter, error) { return &containers.PartitionIter{containers.Partition("none")},nil }
func (s *SimpleTable) PartitionRows(c *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	if s.ut==nil {
		ut,err := s.UDB.UTable(s.InnerName)
		if err!=nil { return nil,err }
		s.ut = ut
	}
	
	iter := s.ut.Iter()
	
	return &simpleTableIter{s,iter},nil
}
var _ sql.Table = (*SimpleTable)(nil)

func val2key(val interface{}) (key []byte,err error) {
	switch v := val.(type) {
	case nil: key = []byte{}
	case []byte: key = v
	case string: key = []byte(v)
	default: err = EBadKeyFormat
	}
	return
}

func (s *SimpleTable) readRecordRaw(key []byte) ([]byte,error) {
	if s.ut==nil {
		ut,err := s.UDB.UTable(s.InnerName)
		if err!=nil { return nil,err }
		s.ut = ut
	}
	return s.ut.Read(key),nil
}

func (s *SimpleTable) Insert(ctx *sql.Context, row sql.Row) error {
	if s.ut==nil {
		ut,err := s.UDB.UTable(s.InnerName)
		if err!=nil { return err }
		s.ut = ut
	}
	iv := preprocess.InsertValue(ctx)
	
	switch iv {
	case "","insert","replace":
		for i,cell := range row {
			if cell!=nil { continue }
			cell = s.ItsSchema[i].Default
			if cell!=nil { continue }
			if s.ItsSchema[i].Nullable { continue }
			return ENullConstraint
		}
	}
	key,err := val2key(row[s.PrimaryKey])
	if err!=nil { return err }
	
	value := s.ut.Read(key)
	switch iv {
	case "","insert":
		if len(value)!=0 { return EDuplicateKey }
	case "delete":
		return s.ut.Write(key,nil)
	}
	var ent msgp.Entity
	
	if len(value)!=0 {
		ent.UnmarshalOrInit(value)
	} else {
		ent.Init()
	}
	ent.SetRow(s.ItsSchema,row)
	
	value,err = ent.Marshal()
	if err!=nil { return err }
	
	return s.ut.Write(key,value)
}
var _ sql.Inserter = (*SimpleTable)(nil)



type simpleTableIter struct{
	inner *SimpleTable
	iter  lstore.UIterator
}
func (s *simpleTableIter) Close() error {
	if s.iter!=nil {
		s.iter.Release()
		s.iter = nil
	}
	return nil
}
func (s *simpleTableIter) Next() (sql.Row,error) {
	var ent msgp.Entity
restart:
	if s.iter==nil {
		return nil,io.EOF
	}
	if s.iter.Next() {
		s.iter = nil
		return nil,io.EOF
	}
	
	err := ent.Unmarshal(s.iter.Value())
	
	// On error, skip the record.
	if err!=nil { goto restart }
	
	row,err := ent.GetRow(s.inner.ItsSchema)
	
	// On error, skip the record.
	if err!=nil { goto restart }
	
	return row,nil
}

func LoadDatabase(db_name string, tx lstore.UDB) (sql.Database,error) {
	db,err := tx.UTable("catalog-tables")
	if err!=nil { return nil,err }
	iter := db.Iter()
	cntnr := new(containers.Database)
	cntnr.SetName(db_name)
	for iter.Next() {
		var dbs TableMetadata
		err = msgp.Unmarshal(iter.Value(),&dbs)
		if err!=nil { return nil,err }
		// fmt.Println(dbs)
		
		name := dbs.Master.Name
		schema := dbs.Master.Schema.ToSqlSchema(name)
		state := 0
		
		switch dbs.Format {
		case "","simple":
			if len(dbs.PrimaryKey)!=1 { state = 1; break }
			cntnr.Add(&SimpleTable{
				UDB: tx,
				InnerName: name,
				ItsName: name,
				ItsSchema: schema,
				PrimaryKey: dbs.PrimaryKey[0],
				PKName: dbs.Master.PKey[0],
			})
			continue
		default:
			state = 2
		}
		switch state {
		case 1: return nil,fmt.Errorf("Table %q: invalid primary key %v for format=%q",name,dbs.Master.PKey,dbs.Format)
		case 2: return nil,fmt.Errorf("Table %q: unknown format %q",name,dbs.Format)
		}
		return nil,fmt.Errorf("Table %q: unknown error",dbs.Master.Name)
	}
	
	return cntnr,nil
}


// ##

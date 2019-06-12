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
Extension to the package sqle/sql.
*/
package badgdb

import "github.com/mad-day/hobbydb/modern/containers"
import "github.com/src-d/go-mysql-server/sql"
import "github.com/mad-day/hobbydb/modern/sqlx"
import "github.com/mad-day/hobbydb/modern/schema"
import "github.com/vmihailenco/msgpack"
import "github.com/dgraph-io/badger"
import "fmt"
import "strings"
import "bytes"
import "io"


type multiTable struct {
	master *schema.TableMasterMetadata
	db     *badger.DB
	update bool
}
func (m *multiTable) StartTransaction() sqlx.DynamicSchemaTableTx {
	txn := m.db.NewTransaction(m.update)
	return &masterTable{master:m.master,txn:txn}
}
var _ sqlx.DynamicTransactionalTable = (*multiTable)(nil)

type masterTable struct {
	master *schema.TableMasterMetadata
	txn    *badger.Txn
}
type tableIndex struct {
	*masterTable
	id,db,name string
}

func (m *masterTable) val2key(key []interface{}) []byte {
	if len(key)==1 {
		k := key[0]
		switch v := k.(type) {
		case []byte: return v
		case string: return []byte(v)
		}
		
		if s,ok := m.master.Schema.Find(m.master.PKey[0]); ok {
			v,e := s.Type.SqlType().SQL(k)
			if e==nil { return v.Raw() }
		}
		
		return []byte(fmt.Sprint(k))
	}
	panic("multi-key not implemented")
}
func (m *masterTable) Commit() error {
	if m.txn==nil { return nil }
	defer m.txn.Discard()
	err := m.txn.Commit()
	m.txn = nil
	return err
}
func (m *masterTable) CommitWith(f func(e error)) {
	if m.txn==nil { return }
	m.txn.CommitWith(f)
	m.txn = nil
}
func (m *masterTable) Discard() {
	if m.txn==nil { return }
	m.txn.Discard()
	m.txn = nil
}


func CreateTransaction(master *schema.TableMasterMetadata,txn *badger.Txn) sqlx.DynamicSchemaTableTx {
	return &masterTable{master:master,txn:txn}
}

var _ sqlx.DynamicSchemaTableTx = (*masterTable)(nil)
type schemaTable struct{
	*masterTable
	
	slave  *schema.TableMetadata
	schema sql.Schema
}

func (s *masterTable) GetTableOf(id,db string,tmd *schema.TableMetadata) (tab sql.Table,idx []sql.Index,err error) {
	if !s.master.CompatibleWith(tmd) { err = fmt.Errorf("Client schema incompatible with master table"); return }
	
	tab = &schemaTable{
		masterTable:s,
		slave: tmd,
		schema:tmd.SqlSchema(),
	}
	
	idx = []sql.Index{&tableIndex{s,id,db,tmd.Name}}
	return
}
func (s *masterTable) Has(partition sql.Partition, key ...interface{}) (bool, error) { return true,nil }
func (s *masterTable) Expressions() []string { return s.master.PKey }
func (s *masterTable) Driver() string { return "primary_key" }

func (s *tableIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	kb := s.val2key(key)
	return &containers.PrimaryKeyLookup{
		Index: s.id,
		Key: kb,
	},nil
}
func (s *tableIndex) ID() string { return s.id }
func (s *tableIndex) Database() string { return s.db }
func (s *tableIndex) Table() string { return s.name }

func (s *schemaTable) Name() string { return s.slave.Name }
func (s *schemaTable) String() string { return s.slave.String() }
func (s *schemaTable) Schema() sql.Schema { return s.schema }
func (s *schemaTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &containers.PartitionIter{containers.Partition("1")},nil
}
func (s *schemaTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	
	panic("...")
}
func (s *schemaTable) IsPrimaryKey(column string) bool {
	for _,pke := range s.master.PKey { if strings.ToLower(pke)==strings.ToLower(column) { return true } }
	return false
}
func (s *schemaTable) decode(val []byte) (row sql.Row, err error) {
	var m map[string]interface{}
	err = msgpack.NewDecoder(bytes.NewBuffer(val)).UseDecodeInterfaceLoose(true).Decode(&m)
	if err!=nil { return }
	
	row = make(sql.Row,len(s.schema))
	for i,c := range s.schema {
		var v interface{}
		v,err = c.Type.Convert(m[c.Name])
		if err!=nil { return }
		if v==nil && c.Default!=nil { v = c.Default }
		row[i] = v
	}
	return
}
func (s *schemaTable) decodeItem(item *badger.Item) (row sql.Row, err error) {
	err = item.Value( func(b []byte) (ne error) {
		row,ne = s.decode(b)
		return
	})
	return
}

type allIter struct{
	*schemaTable
	iter *badger.Iterator
}
func (a *allIter) Close() error {
	if a.iter == nil { return nil }
	defer a.iter.Close()
	a.iter = nil
	return nil
}
func (a *allIter) Next() (row sql.Row, err error) {
	if !a.iter.Valid() { return nil,io.EOF }
restart:
	a.iter.Next()
	if !a.iter.Valid() { return nil,io.EOF }
	row,err = a.decodeItem(a.iter.Item())
	if err!=nil { err = nil; goto restart }
	return
}



// ##

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

Deprecated.
*/
package badgdb

import "github.com/mad-day/hobbydb/modern/containers"
import "github.com/src-d/go-mysql-server/sql"
import "github.com/mad-day/hobbydb/modern/sqlx"
import "github.com/mad-day/hobbydb/modern/schema"
import "github.com/mad-day/hobbydb/modern/preprocess"
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

func NewDynamicTransactionalTable(db *badger.DB, master *schema.TableMasterMetadata) (sqlx.DynamicTransactionalTable,error) {
	if len(master.PKey)!=1 {
		return nil,fmt.Errorf("invalid primary key")
	}
	return &multiTable{master,db,true},nil
}

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
		if k==nil { return []byte("<nil>") }
		pr := []byte("''")
		switch v := k.(type) {
		case []byte: return append(pr,v...)
		case string: return append(pr,v...)
		}
		if s,ok := m.master.Schema.Find(m.master.PKey[0]); ok {
			v,e := s.Type.SqlType().SQL(k)
			if e==nil { return append(pr,v.Raw()...) }
		}
		
		return []byte(fmt.Sprint("''",k))
	}
	buf := new(bytes.Buffer)
	for _,k := range key {
		buf.WriteByte(',')
		if k==nil { continue }
		buf.WriteByte(' ')
		switch k.(type) {
		case []byte,string:
			fmt.Fprintf(buf,"%q",k); continue
		case uint8,uint16,uint32,uint64:
			fmt.Fprintf(buf,"%x",k); continue
		case int8,int16,int32,int64:
			fmt.Fprintf(buf,"%d",k); continue
		}
		if s,ok := m.master.Schema.Find(m.master.PKey[0]); ok {
			v,e := s.Type.SqlType().SQL(k)
			if e==nil {
				buf.Write(v.Raw())
				continue
			}
		}
		fmt.Fprintf(buf,"%v",k)
	}
	return buf.Bytes()
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


func CreateTransaction(master *schema.TableMasterMetadata,txn *badger.Txn) (sqlx.DynamicSchemaTableTx,error) {
	// TODO: verification!
	return &masterTable{master:master,txn:txn},nil
}

var _ sqlx.DynamicSchemaTableTx = (*masterTable)(nil)
type schemaTable struct{
	*masterTable
	
	slave  *schema.TableMetadata
	schema sql.Schema
	
	pktr   []int
}

func (s *masterTable) GetTableOf(db string,tmd *schema.TableMetadata) (tab sql.Table,idx []sql.Index,err error) {
	if !s.master.CompatibleWith(tmd) { err = fmt.Errorf("Client schema incompatible with master table"); return }
	
	v := make([]int,len(s.master.PKey))
	for j := range v { v[j] = -1 }
	for i,c := range tmd.Schema {
		for j,n := range s.master.PKey {
			if c.Name==n { v[j] = i }
		}
	}
	for j := range v { if v[j]<0 { err = fmt.Errorf("Missing: primary key"); return } }
	
	tab = &schemaTable{
		masterTable:s,
		slave: tmd,
		schema:tmd.SqlSchema(),
		pktr: v,
	}
	
	idx = []sql.Index{&tableIndex{s,tmd.Name+"_primary_key",db,tmd.Name}}
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
	return &allIter{s,s.txn.NewIterator(badger.DefaultIteratorOptions)},nil
}
func (s *schemaTable) IsPrimaryKey(column string) bool {
	for _,pke := range s.master.PKey { if strings.ToLower(pke)==strings.ToLower(column) { return true } }
	return false
}
func (s *schemaTable) encode(row sql.Row) (key, value []byte, err error) {
	kr := make([]interface{},len(s.pktr))
	for j,i := range s.pktr {
		kr[j] = row[i]
	}
	key = s.val2key(kr)
	
	m := make(map[string]interface{})
	for i,s := range s.schema {
		if row[i]!=nil {
			m[s.Name] = row[i]
		}
	}
	vt := new(bytes.Buffer)
	err = msgpack.NewEncoder(vt).UseCompactEncoding(true).Encode(m)
	if err!=nil { return }
	value = vt.Bytes()
	
	return
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

func (s *schemaTable) merge(nv,ov []byte) (merged []byte, err error) {
	var nm map[string]interface{}
	err = msgpack.NewDecoder(bytes.NewBuffer(nv)).UseDecodeInterfaceLoose(true).Decode(&nm)
	if err!=nil { return }
	var om map[string]interface{}
	err = msgpack.NewDecoder(bytes.NewBuffer(ov)).UseDecodeInterfaceLoose(true).Decode(&om)
	if err!=nil { return }
	
	if om==nil { return nv,nil }
	for _,c := range s.schema {
		om[c.Name] = nm[c.Name]
	}
	
	vt := new(bytes.Buffer)
	err = msgpack.NewEncoder(vt).UseCompactEncoding(true).Encode(om)
	if err!=nil { return }
	merged = vt.Bytes()
	
	return
}
func (s *schemaTable) mergeItem(nv []byte,item *badger.Item) (merged []byte, err error) {
	err = item.Value( func(b []byte) (ne error) {
		merged,ne = s.merge(nv,b)
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


type idxIter struct{
	*schemaTable
	sql.IndexValueIter
}
func (a *idxIter) Next() (row sql.Row, err error) {
restart:
	var key []byte
	var item *badger.Item
	key,err = a.IndexValueIter.Next()
	if err!=nil { return }
	item,err = a.txn.Get(key)
	if err==badger.ErrKeyNotFound { goto restart }
	
	row,err = a.decodeItem(item)
	if err!=nil { err = nil; goto restart }
	return
}

type lookupTable struct{
	*schemaTable
	lookup sql.IndexLookup
}

func (s *schemaTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	return &lookupTable{s,lookup}
}
func (s *schemaTable) IndexLookup() sql.IndexLookup { return nil }
func (s *schemaTable) IndexKeyValues(*sql.Context, []string) (sql.PartitionIndexKeyValueIter, error) { return nil,fmt.Errorf("unsupported") }

func (s *lookupTable) IndexLookup() sql.IndexLookup { return s.lookup }

func (s *lookupTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	ivi,err := s.lookup.Values(p)
	if err!=nil { return nil,err }
	return &idxIter{s.schemaTable,ivi},nil
}

func (s *schemaTable) create(ctx *sql.Context, row sql.Row) error {
	return nil
}
func (s *schemaTable) Insert(ctx *sql.Context, row sql.Row) error {
	icmd,_ := ctx.Value(preprocess.InsertKey()).(string)
	switch icmd {
	case "insert","":
		for i,c := range s.schema {
			v := row[i]
			if v==nil { v = c.Default }
			if v==nil && !c.Nullable { return fmt.Errorf("attempt to insert nullable in non-null") }
		}
		key,value,err := s.encode(row)
		if err!=nil { return err }
		_,err = s.txn.Get(key)
		if err!=badger.ErrKeyNotFound {
			if err==nil { return fmt.Errorf("Duplicate key error") }
			return err
		}
		return s.txn.Set(key,value)
	case "update":
		key,value,err := s.encode(row)
		if err!=nil { return err }
		item,err := s.txn.Get(key)
		if err!=nil { return err }
		value,err = s.mergeItem(value,item)
		if err!=nil { return err }
		return s.txn.Set(key,value)
	case "delete":
		key,_,err := s.encode(row)
		if err!=nil { return err }
		return s.txn.Delete(key)
	}
	return fmt.Errorf("illegal state")
}


// ##

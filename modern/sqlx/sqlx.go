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
package sqlx

import "github.com/src-d/go-mysql-server/sql"
import "github.com/mad-day/hobbydb/modern/schema"
import "sync"

type Tx interface{
	Commit() error
	CommitWith(func(e error))
	Discard()
}

type DynamicTransactionalTable interface {
	StartTransaction() DynamicSchemaTableTx
}

type DynamicSchemaTable interface {
	GetTableOf(id,db string,tmd *schema.TableMetadata) (sql.Table,[]sql.Index,error)
}
type DynamicSchemaTableTx interface {
	DynamicSchemaTable
	Tx
}

type DynamicSchemaDatabase interface {
	// Returns the table, or null, if not exist.
	GetDynamicTable(name string) DynamicSchemaTable
}
type DynamicSchemaDatabaseTx interface {
	DynamicSchemaDatabase
	Tx
}
type DynamicTransactionalDatabase interface {
	GetDynamicTxTable(name string) DynamicTransactionalTable
}

var _ DynamicTransactionalDatabase = (*DTDatabase)(nil)
type DTDatabase struct {
	m map[string]DynamicTransactionalTable
}
func (d *DTDatabase) SetDynamicTable(name string,tab DynamicTransactionalTable) {
	n := make(map[string]DynamicTransactionalTable,len(d.m)+1)
	for k,v := range d.m { n[k] = v }
	n[name] = tab
	d.m = n
}
func (d DTDatabase) GetDynamicTxTable(name string) DynamicTransactionalTable {
	return d.m[name]
}

type DatabaseTx struct{
	DynamicTransactionalDatabase
	tx map[string]DynamicSchemaTableTx
}
func (d *DatabaseTx) GetDynamicTable(name string) DynamicSchemaTable {
	v,ok := d.tx[name]
	if ok { return v }
	r := d.GetDynamicTxTable(name)
	if r!=nil { return nil }
	if d.tx==nil { d.tx = make(map[string]DynamicSchemaTableTx) }
	v = r.StartTransaction()
	d.tx[name] = v
	return v
}

type wtg struct{
	e error
	sync.WaitGroup
}
func (w *wtg) f(e error) {
	defer w.Done()
	if w.e==nil { w.e = e }
}
func (w *wtg) g(f func(e error)) {
	w.Wait()
	f(w.e)
}

var _ DynamicSchemaDatabaseTx = (*DatabaseTx)(nil)
func (d *DatabaseTx) Commit() (err2 error) {
	for _,c := range d.tx {
		err := c.Commit()
		if err2==nil { err2 = err }
	}
	return
}
func (d *DatabaseTx) CommitWith(f func(e error)) {
	w := new(wtg)
	w.Add(1)
	defer w.Done()
	go w.g(f)
	for _,c := range d.tx {
		w.Add(1)
		c.CommitWith(w.f)
	}
}
func (d *DatabaseTx) Discard() {
	for _,c := range d.tx {
		c.Discard()
	}
}

// --

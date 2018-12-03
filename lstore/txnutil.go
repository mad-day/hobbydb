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


package lstore

import "github.com/syndtr/goleveldb/leveldb/opt"

type tximpl interface{
	open(TableDB,error) (UTable,error)
	commit(map[string]UTable) error
	discard(map[string]UTable)
}

type udbWrapper struct {
	tximpl
	inner Database
	tables map[string]UTable
}
func (i *udbWrapper) UTable(name string) (UTable,error) {
	if t := i.tables[name]; t!=nil { return t,nil }
	t,e := i.open(i.inner.Table(name))
	if e!=nil { return nil,e }
	if i.tables==nil { i.tables = make(map[string]UTable) }
	i.tables[name] = t
	return t,nil
}
func (i *udbWrapper) Commit() error {
	ts := i.tables
	i.tables = nil
	return i.commit(ts)
}
func (i *udbWrapper) Discard() {
	ts := i.tables
	i.tables = nil
	i.discard(ts)
}

// ---------------------------

type txnDirect struct{
	ro opt.ReadOptions
	wo opt.WriteOptions
}
func (d txnDirect) open(t TableDB,e error) (UTable,error) {
	if e!=nil { return nil,e }
	ut := new(uTableD)
	ut.ro,ut.r,ut.wo,ut.w = d.ro,t,d.wo,t
	return ut,nil
}
func (d txnDirect) commit(map[string]UTable) error { return nil }
func (d txnDirect) discard(map[string]UTable) { }
func Simplistic(db Database) UDB { return &udbWrapper{txnDirect{},db,nil} }

// ---------------------------

type Flags uint
func (f Flags) Has(o Flags) bool { return (f&o)==o }

const (
	F_NoSnapshot    Flags = 1<<iota
	F_IgnoreWrites
	F_DiscardWrites
	F_NoCheck
	F_ReRead
	F_TxIgnoreRead
)

// These flags are to be choosen depending on the Low Level Database impl.
const (
	O_ConcurrentCommit Flags = 1<<iota
	O_UseTransaction
)

type ReadIso uint8
const (
	// Snapshot-Isolation
	READ_SNAPSHOT ReadIso = iota

	// Repeatable-Read
	READ_REPEATABLE

	// Read-Committed or Read-Uncommitted
	READ_ANY
)

type WriteIso uint8
const (
	// Transactional Write with commit with conflict-checks.
	WRITE_CHECKED WriteIso = iota

	// Transactional Write with commit.
	WRITE_COMMIT

	// Writes are applied instantly but atomically.
	WRITE_INSTANT_ATOMIC

	// Writes are applied instantly.
	WRITE_INSTANT

	// Read-Only.
	WRITE_DISABLED
)


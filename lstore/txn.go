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

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"errors"
)

var ERO = errors.New("ERO")
var ErrConcurrentUpdate = errors.New("ErrConcurrentUpdate")

type BasicReader interface{
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}
type BasicWriter interface{
	BasicReader
	Put(key, value []byte, wo *opt.WriteOptions) error
	Delete(key []byte, wo *opt.WriteOptions) error
	Write(batch *leveldb.Batch, wo *opt.WriteOptions) error
}

type TableSnapshot interface{
	BasicReader
	Release()
}

type TableTx interface{
	BasicWriter
	Commit() error
	Discard()
}

type TableDB interface{
	BasicWriter
	Snapshot() (TableSnapshot,error)
	Begin() (TableTx,error)
}
type Database interface{
	Table(name string) (TableDB,error)
}


// ----------------------------------------------------

/*
User's database.
*/
type UDB interface{
	UTable(name string) (UTable,error)
	Commit() error
	Discard()
}
type UTable interface{
	Read(key []byte) []byte
	Write(key,value []byte) error
}

type UDBM interface{
	StartTx(r ReadIso, w WriteIso) UDB
}


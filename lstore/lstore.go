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
	"path/filepath"
	"github.com/syndtr/goleveldb/leveldb"
	lerr "github.com/syndtr/goleveldb/leveldb/errors"
	"sync"
	"os"
)

func assureDir(pth string) error {
	s,err := os.Stat(pth)
	if err!=nil {
		if !os.IsNotExist(err) { return err }
		goto enoent
	}
	if !s.IsDir() { goto enotdir }
enotdir:
	if e2 := os.Remove(pth); e2!=nil { return e2}
enoent:
	return os.Mkdir(pth,600)
}
func load(pth string) (*leveldb.DB,error) {
	err := assureDir(pth)
	if err!=nil { return nil,err }
	db,err := leveldb.OpenFile(pth, nil)
	switch err.(type) {
	case *lerr.ErrCorrupted:
		db,err = leveldb.RecoverFile(pth, nil)
	}
	return db,err
}

type Storage struct {
	sync.Mutex
	Basepath string
	
	tables map[string]*leveldb.DB
}
func (s *Storage) RawTable(name string) (*leveldb.DB,error) {
	s.Lock(); defer s.Unlock()
	if r := s.tables[name]; r!=nil { return r,nil }
	
	ldb,err := load(filepath.Join(s.Basepath,name))
	if err!=nil { return nil,err }
	if s.tables==nil {
		s.tables = make(map[string]*leveldb.DB)
	}
	s.tables[name] = ldb
	return ldb,nil
}
func (s *Storage) Table(name string) (TableDB,error) {
	l,err := s.RawTable(name)
	if err!=nil { return nil,err }
	return levelTable{l},nil
}

type levelTable struct{
	*leveldb.DB
}
func (l levelTable) Begin() (TableTx,error) { return l.OpenTransaction() }
func (l levelTable) Snapshot() (TableSnapshot,error) { return l.GetSnapshot() }
var _ TableDB = levelTable{}


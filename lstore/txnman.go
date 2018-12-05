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
	//"github.com/syndtr/goleveldb/leveldb/util"
	"sync"
	"bytes"
	"sort"
)

func bclone(b []byte) []byte {
	if len(b)==0 { return nil }
	c := make([]byte,len(b))
	copy(c,b)
	return c
}

type uIterator struct{
	iter iterator.Iterator
	state uint8
}
func (i *uIterator) Seek(key []byte) (ok bool) {
	ok = i.iter.Seek(key)
	if ok { i.state = 1 } else { i.state = 2 }
	return
}
func (i *uIterator) Next() (ok bool) {
	switch i.state {
	case 0: ok = i.iter.First()
	case 1: ok = i.iter.Next()
	}
	if ok { i.state = 1 } else { i.state = 2 }
	return
}
func (i *uIterator) Key() []byte {
	return i.iter.Key()
}
func (i *uIterator) Value() []byte {
	return i.iter.Value()
}
func (i *uIterator) Release() {
	i.iter.Release()
}

var _ UIterator = (*uIterator)(nil)

type uIteratorAug struct{
	iter iterator.Iterator
	state uint8
	list,ptr [][]byte
}
func (i *uIteratorAug) Seek(key []byte) (ok bool) {
	ok = i.iter.Seek(key)
	j := sort.Search(len(i.list),func(k int) bool { return bytes.Compare(i.list[k],key)>=0 })
	i.ptr = i.list[j:]
	if ok { i.state = 1 } else {
		if len(i.ptr)>0 {
			i.state = 2
		} else {
			i.state = 3
		}
	}
	return
}
func (i *uIteratorAug) Next() (ok bool) {
	switch i.state {
	case 0:
		i.ptr = i.list
		ok = i.iter.First()
		if !ok && len(i.ptr)>0 {
			i.state = 2
			return true
		}
	case 1:
		if len(i.ptr)>0 {
			if bytes.Compare(i.ptr[0],i.iter.Key())<=0 {
				i.ptr = i.ptr[1:]
				ok = true
			}
		}
		if !ok { ok = i.iter.Next() }
	case 2:
		if len(i.ptr)>0 {
			i.ptr = i.ptr[1:]
			ok = len(i.ptr)>0
		}
		return
	}
	if ok { i.state = 1 } else { i.state = 2 }
	return
}
func (i *uIteratorAug) Key() []byte {
	if i.state==2 { if len(i.ptr)==0 { return nil } else { return i.ptr[0] } }
	k := i.iter.Key()
	if len(i.ptr)>0 { if bytes.Compare(i.ptr[0],k)<=0 { return i.ptr[0] } }
	return k
}
func (i *uIteratorAug) Value() []byte {
	return i.iter.Value()
}
func (i *uIteratorAug) Release() {
	i.iter.Release()
}

var _ UIterator = (*uIteratorAug)(nil)

type uTableRO struct{
	ro opt.ReadOptions
	r BasicReader
	itsSN TableSnapshot
}
func (t *uTableRO) Read(key []byte) []byte {
	r,_ :=  t.r.Get(key,&t.ro)
	return r
}
func (t *uTableRO) Write(key,value []byte) error { return ERO }
func (t *uTableRO) Iter() UIterator {
	iter := t.r.NewIterator(nil,&t.ro)
	return &uIterator{iter:iter}
}

type uTableD struct{
	uTableRO
	wo opt.WriteOptions
	w BasicWriter
}
func (t *uTableD) Write(key,value []byte) error {
	if len(value)==0 {
		return t.w.Delete(key,&t.wo)
	}
	return t.w.Put(key,value,&t.wo)
}

type uTableDs struct{
	uTableRO
	wo opt.WriteOptions
	w BasicWriter
	wp *sync.RWMutex
}
func (t *uTableDs) Write(key,value []byte) error {
	t.wp.RLock(); defer t.wp.RUnlock()
	if len(value)==0 {
		return t.w.Delete(key,&t.wo)
	}
	return t.w.Put(key,value,&t.wo)
}

type uTableSR struct{
	uTableRO
	f Flags
	rm map[string][]byte
	w map[string][]byte
	k [][]byte
	sorted bool
	tt TableDB
}
func (t *uTableSR) Read(key []byte) []byte {
	if t.rm==nil { t.rm = make(map[string][]byte) }
	if b,ok := t.w[string(key)]; ok { return bclone(b) }
	if !t.f.Has(F_ReRead) {
		if b,ok := t.rm[string(key)]; ok { return bclone(b) }
	}
	r,_ :=  t.r.Get(key,&t.ro)
	
	if !t.f.Has(F_TxIgnoreRead) {
		t.rm[string(key)] = bclone(r)
	}
	return r
}
func (t *uTableSR) Write(key,value []byte) error {
	if t.f.Has(F_DiscardWrites) { return ERO }
	if t.w==nil { t.w = make(map[string][]byte) }
	if ok,_ := t.r.Has(key,&t.ro); !ok {
		if _,ok := t.w[string(key)]; !ok {
			t.k = append(t.k,bclone(key))
			t.sorted = false
		}
	}
	t.w[string(key)] = bclone(value)
	return nil
}

func (t *uTableSR) Iter() UIterator {
	if !t.sorted {
		sort.Slice(t.k,func(i,j int)bool {
			return bytes.Compare(t.k[i],t.k[j])<0
		})
		t.sorted = true
	}
	iter := t.r.NewIterator(nil,&t.ro)
	return &uIteratorSR{&uIteratorAug{iter:iter,list:t.k},t}
}

type uIteratorSR struct{
	UIterator
	tab *uTableSR
}
func (i *uIteratorSR) Seek(key []byte) bool {
	if !i.UIterator.Seek(key) { return false }
	if b,ok := i.tab.w[string(i.UIterator.Key())]; ok && len(b)==0 { return i.Next() }
	return true
}
func (i *uIteratorSR) Next() bool {
	for i.UIterator.Next() {
		// If the record is deleted, continue.
		if b,ok := i.tab.w[string(i.UIterator.Key())]; ok && len(b)==0 { continue }
		return true
	}
	return false
}
func (i *uIteratorSR) Value() []byte {
	key := i.UIterator.Key()
	if i.tab.rm==nil { i.tab.rm = make(map[string][]byte) }
	if b,ok := i.tab.w[string(key)]; ok { return bclone(b) }
	if !i.tab.f.Has(F_ReRead) {
		if b,ok := i.tab.rm[string(key)]; ok { return bclone(b) }
	}
	r := i.UIterator.Value()
	
	if !i.tab.f.Has(F_TxIgnoreRead) {
		i.tab.rm[string(key)] = bclone(r)
	}
	return r
}

type uTableIW struct{
	uTableSR
	outopt opt.WriteOptions
	writer *sync.RWMutex
	optim Flags
}
func (t *uTableIW) Write(key,value []byte) (rerr error) {
	if t.optim.Has(O_ConcurrentCommit) {
		t.writer.RLock(); defer t.writer.RUnlock()
	} else {
		t.writer.Lock(); defer t.writer.Unlock()
	}
	var myw BasicWriter = t.tt
	if t.optim.Has(O_UseTransaction) {
		if tx,err := t.tt.Begin(); err!=nil {
			return err
		} else {
			defer func () {
				if rerr==nil {
					rerr = tx.Commit()
				} else {
					tx.Discard()
				}
			}()
			myw = tx
		}
		
	}
	ov,ok := t.w[string(key)]
	if !ok { ov,ok = t.rm[string(key)] }
	if ok {
		ev,_ := myw.Get(key,&t.ro)
		if !bytes.Equal(ov,ev) { rerr = ErrConcurrentUpdate ; return }
	}
	if err := myw.Put(key,value,&t.outopt); err!=nil { rerr = err ; return }
	t.uTableSR.Write(key,value)
	return
}


// --------------------------------------------------------------------------



type txManager struct{
	writer sync.RWMutex
	inner Database
	ro opt.ReadOptions
	wo opt.WriteOptions
	optim Flags
}

func Complex(db Database,optim Flags) UDBM {
	return &txManager{inner:db,optim:optim}
}

func (m *txManager) StartTx(r ReadIso, w WriteIso) UDB {
	var txm tximpl
	var f Flags
	switch w {
	case WRITE_CHECKED:
		switch r {
		case READ_REPEATABLE:
			f |= F_NoSnapshot
		case READ_ANY:
			f |= F_NoSnapshot | F_ReRead
		}
	case WRITE_COMMIT:
		switch r {
		case READ_SNAPSHOT:
			f |= F_TxIgnoreRead
		case READ_REPEATABLE:
			f |= F_NoSnapshot | F_NoCheck
		case READ_ANY:
			// F_TxIgnoreRead has the same effect as F_NoCheck | F_ReRead
			f |= F_NoSnapshot | F_TxIgnoreRead
		}
	case WRITE_INSTANT_ATOMIC:
		switch r {
		case READ_REPEATABLE:
			f |= F_NoSnapshot
		case READ_ANY:
			f |= F_NoSnapshot | F_ReRead
		}
		txm = &txManagerReckless{m,f}
	case WRITE_INSTANT:
		switch r {
		case READ_SNAPSHOT:
			txm = &txManagerReckless{m,f}
		case READ_REPEATABLE:
			f |= F_NoSnapshot
			txm = &txManagerReckless{m,f}
		case READ_ANY:
			txm = (*txManagerDirect)(m)
		}
	case WRITE_DISABLED:
		switch r {
		case READ_SNAPSHOT:
			txm = (*txManagerSnapshot)(m)
		case READ_REPEATABLE:
			f |= F_NoSnapshot | F_DiscardWrites
		case READ_ANY:
			txm = (*txManagerReadOnly)(m)
		}
	}
	if txm==nil {
		txm = &txManagerSerializable{m,f}
	}
	return &udbWrapper{txm,m.inner,nil}
}

// --------------------------------------------------------------------------

type txManagerDirect txManager

func (m *txManagerDirect) open(t TableDB,e error) (UTable,error) {
	if e!=nil { return nil,e }
	ut := new(uTableDs)
	ut.ro,ut.wo,ut.wp = m.ro,m.wo,&m.writer
	ut.r,ut.w = t,t
	return ut,nil
}
func (m *txManagerDirect) commit(map[string]UTable) error { return nil }
func (m *txManagerDirect) discard(map[string]UTable) { }


type txManagerReckless struct{
	*txManager
	f Flags
}

func (m *txManagerReckless) open(t TableDB,e error) (UTable,error) {
	if e!=nil { return nil,e }
	ut := new(uTableIW)
	ut.ro = m.ro
	ut.f = m.f
	ut.tt = t
	ut.outopt = m.wo
	ut.writer = &m.writer
	ut.optim = m.optim
	if m.f.Has(F_NoSnapshot) {
		ut.r = t
	} else {
		sn,e := t.Snapshot()
		if e!=nil { return nil,e }
		ut.r,ut.itsSN = sn,sn
	}
	return ut,nil
}
func (m *txManagerReckless) commit(utm map[string]UTable) error {
	for _,ut := range utm {
		sr := ut.(*uTableIW)
		if sr.itsSN!=nil {
			sr.itsSN.Release()
			sr.itsSN = nil
		}
	}
	return nil
}
func (m *txManagerReckless) discard(utm map[string]UTable) {
	for _,ut := range utm {
		sr := ut.(*uTableIW)
		if sr.itsSN!=nil {
			sr.itsSN.Release()
			sr.itsSN = nil
		}
	}
}

type txManagerSnapshot txManager

func (m *txManagerSnapshot) open(t TableDB,e error) (UTable,error) {
	if e!=nil { return nil,e }
	sn,e := t.Snapshot()
	if e!=nil { return nil,e }
	ut := new(uTableRO)
	ut.ro = m.ro
	ut.r,ut.itsSN = sn,sn
	return ut,nil
}
func (m *txManagerSnapshot) commit(utm map[string]UTable) error {
	for _,ut := range utm {
		ut.(*uTableRO).itsSN.Release()
	}
	return nil
}
func (m *txManagerSnapshot) discard(utm map[string]UTable) {
	for _,ut := range utm {
		ut.(*uTableRO).itsSN.Release()
	}
}

type txManagerReadOnly txManager

func (m *txManagerReadOnly) open(t TableDB,e error) (UTable,error) {
	if e!=nil { return nil,e }
	ut := new(uTableRO)
	ut.ro = m.ro
	ut.r = t
	return ut,nil
}
func (m *txManagerReadOnly) commit(map[string]UTable) error { return nil }
func (m *txManagerReadOnly) discard(map[string]UTable) { }

type txManagerSerializable struct{
	*txManager
	f Flags
}
func (m *txManagerSerializable) open(t TableDB,e error) (UTable,error) {
	if e!=nil { return nil,e }
	ut := new(uTableSR)
	ut.ro = m.ro
	ut.f = m.f
	ut.tt = t
	if m.f.Has(F_NoSnapshot) {
		ut.r = t
	} else {
		sn,e := t.Snapshot()
		if e!=nil { return nil,e }
		ut.r,ut.itsSN = sn,sn
	}
	return ut,nil
}
func (m *txManagerSerializable) discard(utm map[string]UTable) {
	for _,ut := range utm {
		sr := ut.(*uTableSR)
		if sr.itsSN!=nil {
			sr.itsSN.Release()
			sr.itsSN = nil
		}
	}
}
func (m *txManagerSerializable) commit(utm map[string]UTable) error {
	for _,ut := range utm {
		sr := ut.(*uTableSR)
		if sr.itsSN!=nil {
			sr.itsSN.Release()
			sr.itsSN = nil
		}
	}
	
	if m.f.Has(F_DiscardWrites) { return nil }
	
	concurrent_commit := m.optim.Has(O_ConcurrentCommit)
	
	if concurrent_commit {
		// In order to qualify for concurrent commit, we must assure, that we only
		// update one table in the transaction.
		count := 0
		for range utm {
			count++
			if count > 1 { concurrent_commit = false; break }
		}
	}
	
	if concurrent_commit {
		// If we have concurrent commit, acquire a shared lock.
		m.writer.RLock(); defer m.writer.RUnlock()
	} else {
		// otherwise, we must acquire an exclusive lock.
		m.writer.Lock(); defer m.writer.Unlock()
	}
	var gerr error
	myws := make(map[string]BasicWriter)
	
	var batch leveldb.Batch
	// Step 1: Check all dependencies. Fail if they're not fullfilled.
	for tabnam,ut := range utm {
		sr := ut.(*uTableSR)
		var myw BasicWriter = sr.tt
		if m.optim.Has(O_UseTransaction) {
			myw,gerr = sr.tt.Begin()
			if gerr!=nil { goto loopdone }
		}
		myws[tabnam] = myw

		// F_NoCheck: Always commit the changes, ignoring conflicts!
		if m.f.Has(F_NoCheck) { continue }

		for key,value := range sr.rm {
			v,_ := myw.Get([]byte(key),&m.ro)
			if !bytes.Equal(value,v) { gerr = ErrConcurrentUpdate; goto loopdone }
		}
	}
	// Step 2: Apply all changes.
	for tabnam,ut := range utm {
		sr := ut.(*uTableSR)
		myw := myws[tabnam]
		for key,value := range sr.w {
			if len(value)==0 {
				batch.Delete([]byte(key))
			} else {
				batch.Put([]byte(key),value)
			}
		}
		gerr = myw.Write(&batch,&m.wo)
		if gerr!=nil { break }
		batch.Reset()
	}
	loopdone:
	// Step 3: Commit transactions, if transactions are used in underlying
	//         datastore.
	if m.optim.Has(O_UseTransaction) {
		if gerr==nil {
			for _,myw := range myws { myw.(TableTx).Commit() }
		} else {
			for _,myw := range myws { myw.(TableTx).Discard() }
		}
	}
	return gerr
}

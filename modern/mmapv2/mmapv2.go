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
The MMAPv2 storage engine.

The primary key is always the first column, or, the first column is always the primary key.
Also, the primary key is auto_increment and is always overriden by the storage engine (you
can't specify an primary key on insert, the table will replace it). The primary key is
a uint32 (INTEGER UNSIGNED in mysql).

The storage engine is called 'MMAPv2' in order to prevent confusion with MongoDB's
original storage engine called 'MMAPv1'.
*/
package mmapv2

import "github.com/mad-day/hobbydb/modern/msgp"
import "github.com/maxymania/go-unstable/bbolt"
import "github.com/spf13/cast"
import "github.com/RoaringBitmap/roaring"
import "encoding/binary"
import "io"
import "fmt"
import "bytes"

var bE = binary.BigEndian

func alibi1(){
	fmt.Println("Hello World")
}

func raw2str(i interface{}) []byte {
	switch i.(type) {
	case nil: return []byte("NULL")
	}
	return []byte(cast.ToString(i))
}
func sortable(i interface{}) bool {
	switch i.(type) {
	case string,[]byte: return true
	}
	return false
}

type ENV struct {
	DB *bbolt.DB
}

type BAT struct {
	env *ENV
	tx *bbolt.Tx
}
func (e *ENV) Begin() (*BAT,error) {
	tx,err := e.DB.Begin(true)
	if err!=nil { return nil,err }
	return &BAT{e,tx},nil
}
func (b *BAT) Commit() error {
	return b.tx.Commit()
}
func (b *BAT) Rollback() error {
	return b.tx.Rollback()
}

type adder struct {
	bbolt.VisitorDefault
	val uint32
}
func (a *adder) vop(value *[]byte) bbolt.VisitOp {
	nb := roaring.NewBitmap()
	if value!=nil {
		_,err := nb.FromBuffer(*value)
		if err!=nil { nb = roaring.NewBitmap() }
	}
	if !nb.CheckedAdd(a.val) { return bbolt.VisitOpNOP() }
	nb.RunOptimize()
	data,err := nb.ToBytes()
	if err!=nil { panic(err) }
	
	return bbolt.VisitOpSET(data)
}

func (a *adder) VisitEmpty(key []byte) bbolt.VisitOp {
	return a.vop(nil)
}
func (a *adder) VisitFull(key, value []byte) bbolt.VisitOp {
	return a.vop(&value)
}

type remover struct {
	bbolt.VisitorDefault
	val uint32
}
func (a *remover) vop(value *[]byte) bbolt.VisitOp {
	nb := roaring.NewBitmap()
	if value!=nil {
		_,err := nb.FromBuffer(*value)
		if err!=nil { nb = roaring.NewBitmap() }
	}
	if !nb.CheckedRemove(a.val) { return bbolt.VisitOpNOP() }
	if nb.IsEmpty() { return bbolt.VisitOpDELETE() }
	nb.RunOptimize()
	data,err := nb.ToBytes()
	if err!=nil { panic(err) }
	return bbolt.VisitOpSET(data)
}

func (a *remover) VisitEmpty(key []byte) bbolt.VisitOp {
	return bbolt.VisitOpNOP()
}
func (a *remover) VisitFull(key, value []byte) bbolt.VisitOp {
	return a.vop(&value)
}


//
type Table struct {
	env *ENV
	name []byte
}
func (e *ENV) Table(name string) *Table {
	return &Table{e,[]byte(name)}
}
func (t *Table) RawInsert(bat *BAT,row []interface{}) error {
	return t.rawWrite(bat,true,row)
}
// DEPRECATED
func (t *Table) RawUpsert(bat *BAT,row []interface{}) error {
	// Delete old version first, if any!
	t.RawDelete(bat,row)
	return t.rawWrite(bat,false,row)
}
func (t *Table) RawUpdate(bat *BAT,oldr,newr []interface{}) error {
	// Delete old version first, if any!
	t.RawDelete(bat,oldr)
	return t.rawWrite(bat,false,newr)
}
func (t *Table) rawWrite(bat *BAT,genkey bool,row []interface{}) error {
	if t.env != bat.env { panic("illegal state: bat.env != env") }
	
	bkt,err := bat.tx.CreateBucketIfNotExists(t.name)
	if err!=nil { return err }
	
	var key uint32
	if genkey {
		seq,err := bkt.NextSequence()
		if err!=nil { return err }
		key = uint32(seq)
		row[0] = key
	} else {
		key,err = cast.ToUint32E(row[0])
		if err!=nil { return err }
	}
	
	val,err := msgp.Marshal(row)
	if err!=nil { return err }
	
	var col [4]byte
	
	ins := &adder{val:key}
	for c,v := range row {
		bE.PutUint32(col[:],uint32(c))
		colbkt,err := bkt.CreateBucketIfNotExists(col[:])
		if err!=nil { return err }
		if c==0 {
			var kb [4]byte
			bE.PutUint32(kb[:],key)
			err = colbkt.Put(kb[:],val)
			if err!=nil { return err }
		} else {
			err = colbkt.Accept(raw2str(v),ins,true)
			if err!=nil { return err }
		}
	}
	
	return nil
}
func (t *Table) RawDelete(bat *BAT,row []interface{}) error {
	if t.env != bat.env { panic("illegal state: bat.env != env") }
	
	var err error
	
	bkt := bat.tx.Bucket(t.name)
	if bkt==nil { return nil } // NOTE: nothing to do!
	//if err!=nil { return err }
	
	var col [4]byte
	
	key := cast.ToUint32(row[0])
	
	ins := &remover{val:key}
	for c,v := range row {
		bE.PutUint32(col[:],uint32(c))
		colbkt := bkt.Bucket(col[:])
		if colbkt==nil { continue } // Nothing to do here. Continue next.
		//if err!=nil { return err }
		if c==0 {
			var kb [4]byte
			bE.PutUint32(kb[:],key)
			err = colbkt.Delete(kb[:])
			if err!=nil { return err }
		} else {
			err = colbkt.Accept(raw2str(v),ins,true)
			if err!=nil { return err }
		}
	}
	return nil
}
func (t *Table) baseIter(bat *BAT) (*baseIterator,error) {
	if bat==nil {
		tx,err := t.env.DB.Begin(false)
		if err!=nil { return nil,err }
		return &baseIterator{tx,tx},nil
	}
	if t.env != bat.env { panic("illegal state: bat.env != env") }
	return &baseIterator{bat.tx,nil},nil
}
func (t *Table) RawIterator(bat *BAT) (TableIterator,error) {
	return t.anyIterator(bat,nil)
}
func (t *Table) anyIterator(bat *BAT,ii roaring.IntIterable) (TableIterator,error) {
	bi,err := t.baseIter(bat)
	if err!=nil { return nil,err }
	bkt := bi.tx.Bucket(t.name)
	if bkt==nil { return bi,nil }
	bkt = bkt.Bucket([]byte{0,0,0,0})
	if bkt==nil { return bi,nil }
	
	if ii!=nil {
		return &bitmapIterator{bi,bkt,ii},nil
	}
	
	return &cursorIterator{bi,0,bkt.Cursor()},nil
}


type TableIterator interface{
	Close() error
	RawNext() ([]interface{},error)
}


type baseIterator struct {
	tx, clo *bbolt.Tx
}
func (b *baseIterator) Close() error {
	if b.clo==nil { return nil }
	err := b.clo.Rollback()
	b.clo = nil
	return err
}
func (b *baseIterator) RawNext() ([]interface{},error) {
	return nil,io.EOF
}

type cursorIterator struct {
	*baseIterator
	state int
	cur *bbolt.Cursor
}
func (b *cursorIterator) String() string { return fmt.Sprint(b.baseIterator,b.cur) }
func (b *cursorIterator) RawNext() (row []interface{},err error) {
	if b.cur==nil { return nil,io.EOF }
	var k,v []byte
	switch b.state {
	case 0:
		k,v = b.cur.First()
		b.state = 1
	default:
		k,v = b.cur.Next()
	}
	if len(k)==0 { b.cur = nil; return nil,io.EOF }
	err = msgp.Unmarshal(v,&row)
	return
}

type bitmapIterator struct {
	*baseIterator
	bkt *bbolt.Bucket
	ii roaring.IntIterable
}
func (b *bitmapIterator) RawNext() (row []interface{},err error) {
restart:
	if !b.ii.HasNext() { return nil,io.EOF }
	i := b.ii.Next()
	var k [4]byte
	bE.PutUint32(k[:],i)
	val := b.bkt.Get(k[:])
	if len(val)==0 { goto restart }
	err = msgp.Unmarshal(val,&row)
	return
}

type TableIndexLookup struct {
	tab *Table
	bi *baseIterator
	bkt *bbolt.Bucket
	impossible bool
	ands []*roaring.Bitmap
	ors  []*roaring.Bitmap
}
func (t *Table) Lookup(bat *BAT) (*TableIndexLookup,error) {
	bi,err := t.baseIter(bat)
	if err!=nil { return nil,err }
	if err!=nil { return nil,err }
	bkt := bi.tx.Bucket(t.name)
	
	ands := make([]*roaring.Bitmap,0,128)
	ors := make([]*roaring.Bitmap,0,128)
	return &TableIndexLookup{t,bi,bkt,bkt==nil,ands,ors},nil
}
func (til *TableIndexLookup) disable() {
	til.bi = nil
}
func (til *TableIndexLookup) Close() error {
	if til.bi==nil { return nil }
	defer til.disable()
	return til.bi.Close()
}

func (til *TableIndexLookup) Equals(col uint32,val interface{}) {
	if til.impossible { return }
	var k [4]byte
	
	if col==0 {
		a := roaring.NewBitmap()
		a.Add(cast.ToUint32(val))
		til.ands = append(til.ands,a)
	} else {
		bE.PutUint32(k[:],col)
		bkt := til.bkt.Bucket(k[:])
		if bkt==nil {
			til.impossible = true
			return
		}
		
		val := bkt.Get(raw2str(val))
		if len(val)==0 {
			til.impossible = true
			return
		}
		a := roaring.NewBitmap()
		_,err := a.FromBuffer(val)
		if err!=nil {
			til.impossible = true
			return
		}
		til.ands = append(til.ands,a)
	}
	
	if len(til.ands)>128 {
		a := roaring.FastAnd(til.ands...)
		til.ands = append(til.ands[:0],a)
	}
}

func (til *TableIndexLookup) EndOr() {
	if len(til.ors)==0 {
		til.impossible = true
		return
	}
	a := roaring.FastOr(til.ors...)
	til.ands = append(til.ands[:0],a)
}
func (til *TableIndexLookup) OrEquals(col uint32,val interface{}) {
	if til.impossible { return }
	var k [4]byte
	
	if col==0 {
		a := roaring.NewBitmap()
		a.Add(cast.ToUint32(val))
		til.ors = append(til.ors,a)
	} else {
		bE.PutUint32(k[:],col)
		bkt := til.bkt.Bucket(k[:])
		if bkt==nil {
			til.impossible = true
			return
		}
		
		val := bkt.Get(raw2str(val))
		if len(val)==0 {
			til.impossible = true
			return
		}
		a := roaring.NewBitmap()
		_,err := a.FromBuffer(val)
		if err!=nil {
			til.impossible = true
			return
		}
		til.ors = append(til.ors,a)
	}
	
	if len(til.ors)>128 {
		a := roaring.FastOr(til.ors...)
		til.ors = append(til.ors[:0],a)
	}
}

func (til *TableIndexLookup) GreaterThan(col uint32,val interface{},orEq bool) (taken bool) {
	if len(til.ors)!=0 { panic("pending OrEquals(), call EndOr first!") }
	
	if til.impossible { return true }
	var k [4]byte
	
	if col==0 { return } // XXX ignore and return a superset.
	
	if !sortable(val) { return } // it is not sortable.
	
	
	bE.PutUint32(k[:],col)
	bkt := til.bkt.Bucket(k[:])
	if bkt==nil {
		til.impossible = true
		return
	}
	
	sk := raw2str(val)
	
	cur := bkt.Cursor()
	nk,nv := cur.Seek(sk)
	
	if !orEq && len(nk)!=0 {
		if bytes.Equal(nk,sk) {
			nk,nv = cur.Next()
		}
	}
	for ; len(nk)!=0 ; nk,nv = cur.Next() {
		
		a := roaring.NewBitmap()
		_,err := a.FromBuffer(nv)
		if err!=nil {
			til.impossible = true
			return
		}
		til.ors = append(til.ors,a)
		if len(til.ors)>128 {
			a := roaring.FastOr(til.ors...)
			til.ors = append(til.ors[:0],a)
		}
	}
	til.EndOr()
	
	if len(til.ands)>128 {
		a := roaring.FastAnd(til.ands...)
		til.ands = append(til.ands[:0],a)
	}
	return
}

func (til *TableIndexLookup) Range(col uint32,fKey, tKey interface{},fEq, tEq bool) (taken bool) {
	if len(til.ors)!=0 { panic("pending OrEquals(), call EndOr first!") }
	
	if til.impossible { return true }
	var k [4]byte
	
	if col==0 { return } // XXX ignore and return a superset.
	
	if !sortable(fKey) { return } // it is not sortable.
	
	
	bE.PutUint32(k[:],col)
	bkt := til.bkt.Bucket(k[:])
	if bkt==nil {
		til.impossible = true
		return
	}
	
	sk := raw2str(fKey)
	tsk := raw2str(tKey)
	
	cur := bkt.Cursor()
	nk,nv := cur.Seek(sk)
	
	if !fEq && len(nk)!=0 {
		if bytes.Equal(nk,sk) {
			nk,nv = cur.Next()
		}
	}
	for ; len(nk)!=0 ; nk,nv = cur.Next() {
		
		if tEq {
			if bytes.Compare(nk,tsk)>0 { break }
		} else {
			if bytes.Compare(nk,tsk)>=0 { break }
		}
		
		a := roaring.NewBitmap()
		_,err := a.FromBuffer(nv)
		if err!=nil {
			til.impossible = true
			return
		}
		til.ors = append(til.ors,a)
		if len(til.ors)>128 {
			a := roaring.FastOr(til.ors...)
			til.ors = append(til.ors[:0],a)
		}
	}
	til.EndOr()
	
	if len(til.ands)>128 {
		a := roaring.FastAnd(til.ands...)
		til.ands = append(til.ands[:0],a)
	}
	return
}

func (til *TableIndexLookup) RawIterator() (TableIterator,error) {
	bi := til.bi
	
	if til.impossible { return bi,nil }
	
	bkt := til.bkt
	if bkt==nil { return bi,nil }
	bkt = bkt.Bucket([]byte{0,0,0,0})
	if bkt==nil { return bi,nil }
	
	if len(til.ands)==0 { return &cursorIterator{bi,0,bkt.Cursor()},nil }
	
	if len(til.ands)>1 {
		a := roaring.FastAnd(til.ands...)
		til.ands = append(til.ands[:0],a)
	}
	
	return &bitmapIterator{bi,bkt,til.ands[0].Iterator()},nil
}

// ##

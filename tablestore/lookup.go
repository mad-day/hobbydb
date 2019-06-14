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
	"github.com/mad-day/hobbydb/modern/msgp"
	"github.com/mad-day/hobbydb/modern/eplan"
	"github.com/src-d/go-mysql-server/sql"
	
	"io"
	"errors"
)

var (
	e_bad_lookup = errors.New("e_bad_lookup")
)

type keyIterator interface {
	Close() error
	Next() ([]byte,error)
}

type keys [][]byte
func (k *keys) Close() error { return nil }
func (k *keys) Next() ([]byte,error) {
	l := *k
	if len(l)==0 { return nil,io.EOF }
	*k = l[1:]
	return l[0],nil
}

type ikeyExtractor interface {
	extract(filter eplan.TableRowFilter) (keyIterator,error)
}

type keyExtractor struct {
	pki  int
	flti int
}
func (k *keyExtractor) extract(filter eplan.TableRowFilter) (keyIterator,error) {
	f := filter[k.pki][k.flti]
	switch f.Op {
	case eplan.TF_Eq,eplan.TF_In:
		var ka keys
		for _,arg := range f.Arg {
			ks,e := val2key(arg.GetValue())
			if e!=nil { return nil,e }
			ka = append(ka,ks)
		}
		return &ka,nil
	}
	return nil,e_bad_lookup
}

type lookupTable struct {
	*SimpleTable
	ikeyExtractor
	filter eplan.TableRowFilter
}
func (i *lookupTable) PartitionRows(c *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	
	ki,err := i.extract(i.filter)
	if err!=nil { return nil,err }
	
	return &lookupIterator{ki,i.SimpleTable},nil
}

type lookupIterator struct {
	keyIterator
	inner *SimpleTable
}
func (l *lookupIterator) Next() (row sql.Row,err error) {
	var ent msgp.Entity
	var key,value []byte
restart:	
	key,err = l.keyIterator.Next()
	if err!=nil { return }
	
	value,err = l.inner.readRecordRaw(key)
	if err!=nil { return }
	
	err = ent.Unmarshal(value)
	
	// On error, skip the record.
	if err!=nil { goto restart }
	
	row,err = ent.GetRow(l.inner.ItsSchema)
	
	// On error, skip the record.
	if err!=nil { goto restart }
	
	return
}

func (s *SimpleTable) GenerateLookupHint(ctx *sql.Context,filter eplan.TableRowFilter) (hint interface{},_ error) {
	value := 0
	
	for i,f := range filter[s.PrimaryKey] {
		switch f.Op {
		case eplan.TF_Eq,eplan.TF_In:
			if value>1 { continue }
			value = 1
			hint = &keyExtractor{s.PrimaryKey,i}
		}
	}
	return
}

func (s *SimpleTable) GetSubsetTable(ctx *sql.Context,hint interface{}, filter eplan.TableRowFilter) (sql.Table,error) {
	if hint==nil {
		nhi,err := s.GenerateLookupHint(ctx,filter)
		if err!=nil { return nil,err }
		hint = nhi
	}
	
	switch v := hint.(type) {
	case *keyExtractor: return &lookupTable{s,v,filter},nil
	}
	
	return s,nil
}

//##

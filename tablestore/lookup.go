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
	"fmt"
)

var (
	e_bad_lookup = errors.New("e_bad_lookup")
)

type unoptimized struct{}
func (unoptimized) ExplainPlan() string { return "<ANY ROW>" }

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
	pki   int
	flti  int
	vals  fmt.Stringer
}
func (k *keyExtractor) ExplainPlan() string { return "PK[] "+(k.vals.String()) }
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

type rangeHint struct {
	col   int
	until []int
	seekto []int
	
	debug []*eplan.TableFilter
}
func (rh *rangeHint) ExplainPlan() string {
	
	var childs []string
	if len(rh.seekto)!=0 {
		childs = append(childs,"SEEK TO "+rh.debug[rh.seekto[0]].String())
	}
	if len(rh.until)!=0 {
		pr2 := sql.NewTreePrinter()
	
		d_until := make([]string,len(rh.until))
		for i,j := range rh.until {
			d_until[i] = rh.debug[j].String()
		}
		switch len(d_until) {
		case 1:
			pr2.WriteNode("WHILE %s",d_until[0])
			childs = append(childs,pr2.String())
		default:
			pr2.WriteNode("WHILE AND")
			pr2.WriteChildren(d_until...)
			childs = append(childs,pr2.String())
		}
	}
	
	pr := sql.NewTreePrinter()
	
	pr.WriteNode("RANGE PK")
	pr.WriteChildren(childs...)
	
	return pr.String()
}

type rangeIterator struct {
	sql.RowIter
	*rangeHint
	raw eplan.TableRowFilter
}
func (r *rangeIterator) Next() (row sql.Row,err error) {
	var ok bool
	row,err = r.RowIter.Next()
	if err!=nil { return }
	
	for _,i := range r.until {
		ok,err = r.raw[r.col][i].Match(row[r.col])
		if err!=nil { return }
		if !ok { return nil,io.EOF }
	}
	return
}

type rangeTable struct {
	*SimpleTable
	*rangeHint
	raw eplan.TableRowFilter
}
func (i *rangeTable) PartitionRows(c *sql.Context, p sql.Partition) (sql.RowIter, error) {
	
	ri,err := i.SimpleTable.PartitionRows(c,p)
	if err!=nil { return nil,err }
	
	if len(i.seekto)==1 {
		if sti,ok := ri.(*simpleTableIter); ok {
			key,err := val2key(i.raw[i.PrimaryKey][i.seekto[0]].Arg[0].GetValue())
			if err==nil {
				sti.iter.Seek(key)
			}
		}
		
		// Sometimes, we only have SEEKTO but no UNTIL. Return Straigth!
		if len(i.until)==0 { return ri,nil }
	}
	
	return &rangeIterator{ri,i.rangeHint,i.raw},nil
}




// ##################### ---------------------------- ############################


func (s *SimpleTable) GenerateLookupHint(ctx *sql.Context,filter eplan.TableRowFilter) (hint interface{},_ error) {
	value := 0
	
	var rh *rangeHint
	var seekto_n,seekto_p int
	for i,f := range filter[s.PrimaryKey] {
		switch f.Op {
		case eplan.TF_Eq,eplan.TF_In:
			if value>2 { continue }
			value = 2
			hint = &keyExtractor{s.PrimaryKey,i,f}
		case eplan.TF_Lt,eplan.TF_Le:
			if value>1 { continue }
			value = 1
			if rh==nil { rh = &rangeHint{col:s.PrimaryKey,debug:filter[s.PrimaryKey]} }
			hint = rh
			rh.until = append(rh.until,i)
		case eplan.TF_Gt,eplan.TF_Ge:
			if seekto_n!=0 { continue } // If we have one, ignore the rest!
			seekto_n++
			seekto_p = i
		}
	}
	if seekto_n>0 && !(value>1) {
		value = 1
		if rh==nil { rh = &rangeHint{col:s.PrimaryKey,debug:filter[s.PrimaryKey]} }
		rh.seekto = []int{seekto_p}
		hint = rh
	}
	
	
	// Drop a Dummy-Hint to prevent GetSubsetTable from calling into this function again!
	if hint==nil { hint = unoptimized{} }
	
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
	case *rangeHint: return &rangeTable{s,v,filter},nil
	}
	
	return s,nil
}


//##

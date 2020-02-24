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



package eplan


import "github.com/src-d/go-mysql-server/sql"
import "github.com/src-d/go-mysql-server/sql/plan"
import "io"


type NestedLoopScan struct{
	basetable
	sql.Table
	
	Left sql.Node
	
	RightOuter bool
	
	RowFilter TableRowFilter
	
	LookupHint interface{}
}
func (l *NestedLoopScan) Schema() sql.Schema { return append(l.Left.Schema(), l.Table.Schema()...) }
func (l *NestedLoopScan) Children() []sql.Node { return []sql.Node{l.Left} }
func (l NestedLoopScan) WithChildren(childs ...sql.Node) (sql.Node, error) {
	l.Left = childs[0]
	return &l,nil
}
func (l *NestedLoopScan) Expressions() []sql.Expression { return l.RowFilter.Expressions() }
func (l NestedLoopScan) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	l.RowFilter = l.RowFilter.WithExpressions(exprs...)
	return &l,nil
}
// DEPRECATED
func (l NestedLoopScan) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	var err error
	l.RowFilter,err = l.RowFilter.TransformExpressions(f)
	if err!=nil { return nil,err }
	
	return &l,nil
}
// DEPRECATED
func (l NestedLoopScan) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	var err error
	
	//l.Left,err = l.Left.TransformExpressionsUp(f)
	//if err!=nil { return nil,err }
	
	l.RowFilter,err = l.RowFilter.TransformExpressions(f)
	if err!=nil { return nil,err }
	
	return &l,nil
}
// DEPRECATED
func (l NestedLoopScan) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	//var err error
	
	//l.Left,err = l.Left.TransformUp(f)
	//if err!=nil { return nil,err }
	
	return f(&l)
}
func (l *NestedLoopScan) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	ri,err := l.Left.RowIter(ctx)
	if err!=nil { return nil,err }
	
	left := len(l.Left.Schema())
	right := len(l.Table.Schema())
	
	ln := &nestedLoopJoin{ri,ctx,left+right,left,l.RightOuter,l.rightLookup,nil,nil}
	
	return ln,nil
}
/*
type nestedLoopJoin struct {
	sql.RowIter
	ctx *sql.Context
	length,offset int
	outer  bool
	provider func(ctx *sql.Context,row sql.Row) (sql.RowIter,error)
	
	left   sql.Row
	right  sql.RowIter
}
*/



func (l *NestedLoopScan) rightLookup(ctx *sql.Context,row sql.Row) (sql.RowIter, error) {
	
	// Hack: Copy the l.RowFilter
	rf,err := l.RowFilter.TransformExpressions(blindXform)
	if err!=nil { return nil,err }
	
	// Set the search key.
	err = rf.Evaluate(ctx,row)
	if err!=nil { return nil,err }
	
	tab := l.Table
	
	if flt,ok := tab.(FastLookupTable) ; ok {
		tab,err = flt.GetSubsetTable(ctx,l.LookupHint,rf)
		if err!=nil { return nil,err }
	}
	
	ri,err := plan.NewResolvedTable(tab).RowIter(ctx)
	if err!=nil { return nil,err }
	
	return &filterRowIter{ri,rf},nil
}

var _ sql.Node = (*NestedLoopScan)(nil)
var _ sql.Expressioner = (*NestedLoopScan)(nil)

func (l *NestedLoopScan) String() string {
	pr := sql.NewTreePrinter()
	if l.RightOuter {
		pr.WriteNode("RIGHT OUTER JOIN (NestedLoopScan)")
	} else {
		pr.WriteNode("INNER JOIN (NestedLoopScan)")
	}
	
	if plan,ok := FastLookupHintExplain(l.LookupHint); ok {
		pr.WriteChildren(l.Left.String(),l.Table.String(), plan, l.RowFilter.String())
	} else {
		pr.WriteChildren(l.Left.String(),l.Table.String(), l.RowFilter.String())
	}
	
	return pr.String()
}


type nestedLoopJoin struct {
	sql.RowIter
	ctx *sql.Context
	length,offset int
	outer  bool
	provider func(ctx *sql.Context,row sql.Row) (sql.RowIter,error)
	
	left   sql.Row
	right  sql.RowIter
}

func (s *nestedLoopJoin) Close() error {
	var e1,e2 error
	e1 = s.RowIter.Close()
	if s.right!=nil { e2 = s.right.Close() }
	
	if e1==nil { e1=e2 }
	return e1
}

func (s *nestedLoopJoin) Next() (row sql.Row, err error) {
	var rrow sql.Row
	
perform:
	if len(s.left)!=0 && s.right!=nil {
		rrow,err = s.right.Next()
		if err!=nil && err!=io.EOF { return }
		if err==nil {
			row = make(sql.Row,s.length)
			copy(row,s.left)
			copy(row[:s.offset],rrow)
			return
		}
		s.right.Close()
		s.right = nil
		s.left = nil
		
		if s.outer {
			row = make(sql.Row,s.length)
			copy(row,s.left)
			// Leave the right side empty!
			return
		}
	}
	
	s.left,err = s.RowIter.Next()
	if err!=nil { return }
	
	s.right,err = s.provider(s.ctx,s.left)
	if err!=nil { return }
	
	goto perform
	
	return
}

// ##

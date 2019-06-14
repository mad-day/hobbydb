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
import "github.com/src-d/go-mysql-server/sql/expression"
import "errors"
import "bytes"
import "fmt"

/*
An intermediate Filter, that hasn't been handled by the query planer rule.
*/
type UnhandledFilter struct {
	plan.Filter
}
func NewUnhandledFilter(expression sql.Expression, child sql.Node) *UnhandledFilter {
	return &UnhandledFilter{*plan.NewFilter(expression,child)}
}

var (
	ErrInternal = errors.New("Internal Error")
)

type TableFilterOp uint
const (
	TF_True TableFilterOp = iota // field = true (boolean field)
	TF_False // field = false (boolean field)
	TF_Eq // '='
	TF_Ne // '!='
	TF_Gt // '>'
	TF_Ge // '>='
	TF_Lt // '<'
	TF_Le // '<='
	
	TF_In // 'IN'
	TF_NotIn // 'NOT IN'
)

func blindXform(expr sql.Expression) (sql.Expression,error) { return expr,nil }

type TableFilterArg struct{
	expr sql.Expression
	
	value interface{}
}
func NewTableFilterArg(expr sql.Expression) *TableFilterArg {
	return &TableFilterArg{expr:expr}
}
func (tfa *TableFilterArg) GetValue() interface{} { return tfa.value }
func (tfa TableFilterArg) TransformExpressions(f sql.TransformExprFunc) (*TableFilterArg, error) {
	if tfa.expr==nil { return &tfa,nil }
	expr,err := tfa.expr.TransformUp(f)
	if err!=nil { return nil,err }
	tfa.expr = expr
	return &tfa,nil
}
func (tfa *TableFilterArg) String() string {
	if tfa.expr==nil { return "<nil>" }
	return tfa.expr.String()
}

// Initializes the RowFilter with the current row.
func (tfa *TableFilterArg) Evaluate(ctx *sql.Context,row sql.Row) (err error) {
	if tfa.expr==nil { return }
	tfa.value,err = tfa.expr.Eval(ctx,row)
	return
}


type TableFilter struct{
	Op TableFilterOp
	Type sql.Type
	
	Arg []*TableFilterArg
}
func NewTableFilterArgs(expr sql.Expression) []*TableFilterArg {
	return []*TableFilterArg{NewTableFilterArg(expr)}
}
func (t TableFilter) TransformExpressions(f sql.TransformExprFunc) (*TableFilter, error) {
	args := make([]*TableFilterArg,len(t.Arg))
	for i,arg := range t.Arg {
		narg,err := arg.TransformExpressions(f)
		if err!=nil { return nil,err }
		args[i] =narg
	}
	t.Arg = args
	
	return &t,nil
}
func (t *TableFilter) Match(value interface{}) (bool,error) {
	switch t.Op {
	case TF_True,TF_False:
		i,err := t.Type.Compare(value,t.Op==TF_True)
		return i==0,err
	case TF_Eq: i,err := t.Type.Compare(value,t.Arg[0].value); return i==0,err
	case TF_Ne: i,err := t.Type.Compare(value,t.Arg[0].value); return i!=0,err
	case TF_Gt: i,err := t.Type.Compare(value,t.Arg[0].value); return i>0,err
	case TF_Ge: i,err := t.Type.Compare(value,t.Arg[0].value); return i>=0,err
	case TF_Lt: i,err := t.Type.Compare(value,t.Arg[0].value); return i<0,err
	case TF_Le: i,err := t.Type.Compare(value,t.Arg[0].value); return i<=0,err
	
	case TF_In:
		for _,arg := range t.Arg {
			i,err := t.Type.Compare(value,arg.value)
			if err!=nil || i==0 { return true,err }
		}
		return false,nil
	case TF_NotIn:
		for _,arg := range t.Arg {
			i,err := t.Type.Compare(value,arg.value)
			if err!=nil || i==0 { return false,err }
		}
		return true,nil
	}
	return true,ErrInternal
}
func (t *TableFilter) String() string {
	b := new(bytes.Buffer)
	b.WriteString("$VAL")
	
	switch t.Op {
	case TF_True: b.WriteString(" TRUE")
	case TF_False: b.WriteString(" FALSE")
	case TF_Eq: fmt.Fprintf(b," = %v",t.Arg[0])
	case TF_Ne: fmt.Fprintf(b," != %v",t.Arg[0])
	case TF_Gt: fmt.Fprintf(b," > %v",t.Arg[0])
	case TF_Ge: fmt.Fprintf(b," >= %v",t.Arg[0])
	case TF_Lt: fmt.Fprintf(b," < %v",t.Arg[0])
	case TF_Le: fmt.Fprintf(b," <= %v",t.Arg[0])
	case TF_In,TF_NotIn:
		switch t.Op {
		case TF_In: b.WriteString(" IN")
		case TF_NotIn: b.WriteString(" NOT IN")
		}
		fmt.Fprintf(b," (")
		for i,arg := range t.Arg {
			if i!=0 { b.WriteString(" ,") }
			b.WriteString(arg.String())
		}
		fmt.Fprintf(b," )")
	}
	
	return b.String()
}

// Initializes the RowFilter with the current row.
func (t *TableFilter) Evaluate(ctx *sql.Context,row sql.Row) (err error) {
	for _,arg := range t.Arg {
		err = arg.Evaluate(ctx,row)
		if err!=nil { break }
	}
	return
}

func tableFiltersString(ts []*TableFilter) string {
	switch len(ts) {
	case 0: return "ANY"
	case 1: return ts[0].String()
	}
	
	pr := sql.NewTreePrinter()
	
	pr.WriteNode("AND")
	
	strs := make([]string,len(ts))
	for i,t := range ts {
		strs[i] = t.String()
	}
	
	pr.WriteChildren(strs...)
	
	return pr.String()
}

type TableRowFilter [][]*TableFilter

func (t TableRowFilter) String() string {
	pr := sql.NewTreePrinter()
	
	pr.WriteNode("ROW")
	
	strs := make([]string,len(t))
	for i,elem := range t {
		strs[i] = tableFiltersString(elem)
	}
	pr.WriteChildren(strs...)
	
	return pr.String()
}

func (t TableRowFilter) Match(row sql.Row) (b bool,e error) {
	b = true
	for i,cf := range t {
		value := row[i]
		for _,cff := range cf {
			nb,ne := cff.Match(value)
			if b { b = nb }
			if e==nil { e = ne }
			if !b || e!=nil { return }
		}
	}
	return
}
func (t TableRowFilter) TransformExpressions(f sql.TransformExprFunc) (nt TableRowFilter, err error) {
	nt = make(TableRowFilter,len(t))
	for i,cf := range t {
		ncf := make([]*TableFilter,len(cf))
		nt[i] = ncf
		for j,cff := range cf {
			ncf[j],err = cff.TransformExpressions(f)
			if err!=nil { return }
		}
	}
	return
}
func (t TableRowFilter) Expressions() (e []sql.Expression) {
	for _,cf := range t {
		for _,ccf := range cf {
			for _,arg := range ccf.Arg {
				if arg.expr==nil { continue }
				e = append(e,arg.expr)
			}
		}
	}
	return
}
func (t TableRowFilter) Add(idx int, tf *TableFilter) bool {
	if idx<0 { return false }
	
	t[idx] = append(t[idx],tf)
	
	return true
}

// Initializes the RowFilter with the current row.
func (t TableRowFilter) Evaluate(ctx *sql.Context,row sql.Row) (err error) {
	for _,cf := range t {
		for _,ccf := range cf {
			err = ccf.Evaluate(ctx,row)
			if err!=nil { return }
		}
	}
	return
}

/*
Shallow clone.
*/
func (t TableRowFilter) Clone() TableRowFilter {
	var e error
	t,e = t.TransformExpressions(blindXform)
	if e!=nil { panic(e) }
	return t
}

/*
Deep clone.
*/
func (t TableRowFilter) DeepClone() TableRowFilter {
	var e error
	t,e = t.TransformExpressions(blindXform)
	if e!=nil { panic(e) }
	return t
}

/*
A function to be supplied

	func fu (expr sql.Expression) int {
		// Return the column-index of the row-field 'expr' is refering
		// Return -1 if 'expr' is not refering any column.
		...
	}
*/
type TableFieldChecker func(sql.Expression) int

func ConvertFilterExpr(expr sql.Expression, fchk TableFieldChecker) (idx int,tf *TableFilter) {
	return convertFilterExpr(false,expr,fchk)
}
func convertFilterExpr(inv bool,expr sql.Expression, fchk TableFieldChecker) (idx int,tf *TableFilter) {
restart:
	switch v := expr.(type) {
	case *expression.Not:
		inv,expr = !inv,v
		goto restart
	case *expression.In,*expression.NotIn:
		co := expr.(expression.Comparer)
		field := co.Left()
		rargs,ok := co.Right().(expression.Tuple)
		if !ok { return }
		fargs := make([]*TableFilterArg,len(rargs))
		for i,rarg := range rargs {
			// If rarg refers the current table, fail.
			if fchk(rarg)>=0 { return }
			fargs[i] = NewTableFilterArg(rarg)
		}
		idx = fchk(field)
		tf = &TableFilter{Arg: fargs,Type: co.Left().Type()}
		if inv {
			switch expr.(type) {
			case *expression.In: tf.Op = TF_NotIn
			case *expression.NotIn: tf.Op = TF_In
			}
		} else {
			switch expr.(type) {
			case *expression.In: tf.Op = TF_In
			case *expression.NotIn: tf.Op = TF_NotIn
			}
		}
		return
	}
	switch v := expr.(type) {
	case expression.Comparer:
		field,arg := v.Left(),v.Right()
		idx = fchk(field)
		if idx<0 {
			field,arg = arg,field
			idx = fchk(field)
		}
		if idx<0 { return }
		tf = &TableFilter{Arg: NewTableFilterArgs(arg),Type: field.Type()}
		if inv {
			switch expr.(type) {
			case *expression.Equals: tf.Op = TF_Ne
			case *expression.GreaterThan: tf.Op = TF_Le
			case *expression.GreaterThanOrEqual: tf.Op = TF_Lt
			case *expression.LessThan: tf.Op = TF_Ge
			case *expression.LessThanOrEqual: tf.Op = TF_Gt
			default: idx,tf = -1,nil
			}
		} else {
			switch expr.(type) {
			case *expression.Equals: tf.Op = TF_Eq
			case *expression.GreaterThan: tf.Op = TF_Gt
			case *expression.GreaterThanOrEqual: tf.Op = TF_Ge
			case *expression.LessThan: tf.Op = TF_Lt
			case *expression.LessThanOrEqual: tf.Op = TF_Le
			default: idx,tf = -1,nil
			}
		}
		return
	}
	
	// Check, if we used expr as boolean condition itself.
	idx = fchk(expr)
	if idx<0 { return }
	tf = new(TableFilter)
	tf.Type = expr.Type()
	if inv {
		tf.Op = TF_NotIn
	} else {
		tf.Op = TF_In
	}
	
	return
}

type basetable struct{}
func (basetable) Children() []sql.Node { return nil }
func (basetable) Resolved() bool { return true }

type TableScan struct{
	basetable
	sql.Table
	
	RowFilter TableRowFilter
	
	LookupHint interface{}
}
func (l TableScan) Expressions() []sql.Expression { return l.RowFilter.Expressions() }
func (l TableScan) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	var err error
	l.RowFilter,err = l.RowFilter.TransformExpressions(f)
	if err!=nil { return nil,err }
	
	return &l,nil
}
func (l TableScan) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	var err error
	l.RowFilter,err = l.RowFilter.TransformExpressions(f)
	if err!=nil { return nil,err }
	
	return &l,nil
}
func (l *TableScan) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) { return f(l) }
func (l *TableScan) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	
	// Hack: Copy the l.RowFilter
	rf,err := l.RowFilter.TransformExpressions(blindXform)
	if err!=nil { return nil,err }
	
	// Set the search key.
	err = rf.Evaluate(ctx,sql.Row{})
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
var _ sql.Node = (*TableScan)(nil)
var _ sql.Expressioner = (*TableScan)(nil)

type filterRowIter struct {
	sql.RowIter
	RowFilter TableRowFilter
}
func (s *filterRowIter) Next() (row sql.Row, err error) {
	var ok bool
restart:
	row,err = s.RowIter.Next()
	if err!=nil { return }
	ok,err = s.RowFilter.Match(row)
	if err!=nil { return }
	if !ok { goto restart }
	return
}

func (l *TableScan) String() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("TableScan")
	
	if plan,ok := FastLookupHintExplain(l.LookupHint); ok {
		pr.WriteChildren(l.Table.String(), plan, l.RowFilter.String())
	} else {
		pr.WriteChildren(l.Table.String(), l.RowFilter.String())
	}
	return pr.String()
}

// ##

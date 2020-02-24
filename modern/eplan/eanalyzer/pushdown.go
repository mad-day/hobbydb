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



package eanalyzer

import "github.com/src-d/go-mysql-server/sql"
import "github.com/src-d/go-mysql-server/sql/analyzer"
import "github.com/src-d/go-mysql-server/sql/expression"
import "github.com/src-d/go-mysql-server/sql/plan"
import "github.com/mad-day/hobbydb/modern/legacy"

import "fmt"

func splitLeftRight(es []sql.Expression,fu func(e sql.Expression) int) (left,right,center []sql.Expression) {
	for _,expr := range es {
		i := 0
		expression.Inspect(expr, func(e sql.Expression) bool{
			i |= fu(e)
			return true
		})
		switch i&3 { 
		case 1: left = append(left,expr)
		case 2: right = append(right,expr)
		default: center = append(center,expr)
		}
	}
	return
}

func splitAt(i int) func(e sql.Expression) int {
	return func(e sql.Expression) int {
		gf,ok := e.(*expression.GetField)
		if !ok { return 0 }
		if gf.Index()<i { return 1 }
		return 2
	}
}

func shiftRight(i int) legacy.TransformExprFunc {
	return func(expr sql.Expression) (sql.Expression, error) {
		gf,ok := expr.(*expression.GetField)
		if !ok { return expr,nil }
		if gf.Index() < i {
			return nil,fmt.Errorf("index out of range: %d not in [%d...Inf]",gf.Index(),i)
		}
		return expression.NewGetFieldWithTable(
			gf.Index()-i,
			gf.Type(),
			gf.Table(),
			gf.Name(),
			gf.IsNullable(),
		),nil
	}
}

func shiftLeft(i int) legacy.TransformExprFunc {
	return func(expr sql.Expression) (sql.Expression, error) {
		gf,ok := expr.(*expression.GetField)
		if !ok { return expr,nil }
		return expression.NewGetFieldWithTable(
			gf.Index()+i,
			gf.Type(),
			gf.Table(),
			gf.Name(),
			gf.IsNullable(),
		),nil
	}
}

func shiftRename(schema sql.Schema) legacy.TransformExprFunc {
	return func(expr sql.Expression) (sql.Expression, error) {
		gf,ok := expr.(*expression.GetField)
		if !ok { return expr,nil }
		if gf.Index() < 0 || gf.Index()>len(schema) {
			return nil,fmt.Errorf("index out of range: %d not in [0...%d]",gf.Index(),len(schema))
		}
		col := schema[gf.Index()]
		return expression.NewGetFieldWithTable(
			gf.Index(),
			col.Type,
			col.Source,
			col.Name,
			col.Nullable,
		),nil
	}
}

func pushdown(node sql.Node) (sql.Node, bool) {
	filter,ok := node.(*plan.Filter)
	if !ok { return nil,false }
	
	var parts []sql.Expression
	splitAnd(&parts,filter.Expression)
	
	var err error
	
	var nleft,nright,result sql.Node
	
	switch v := filter.Child.(type) {
	case *plan.CrossJoin:
		leftsz := len(v.Left.Schema())
		left,right,center := splitLeftRight(parts,splitAt(leftsz))
		for i := range right {
			right[i],err = legacy.TransformUpExpr(right[i],shiftRight(leftsz))
			if err!=nil { panic(err) }
		}
		
		nleft  = v.Left
		nright = v.Right
		
		if len(left)>0 { nleft = plan.NewFilter(expression.JoinAnd(left...),nleft) }
		if len(right)>0 { nright = plan.NewFilter(expression.JoinAnd(right...),nright) }
		
		result = plan.NewCrossJoin(nleft,nright)
		
		if len(center)>0 { result = plan.NewFilter(expression.JoinAnd(center...),result) }
		
		return result,true
	case *plan.InnerJoin:
		leftsz := len(v.Left.Schema())
		left,right,center := splitLeftRight(parts,splitAt(leftsz))
		for i := range right {
			right[i],err = legacy.TransformUpExpr(right[i],shiftRight(leftsz))
			if err!=nil { panic(err) }
		}
		
		nleft  = v.Left
		nright = v.Right
		
		if len(left)>0 { nleft = plan.NewFilter(expression.JoinAnd(left...),nleft) }
		if len(right)>0 { nright = plan.NewFilter(expression.JoinAnd(right...),nright) }
		
		result = plan.NewInnerJoin(nleft,nright,v.Cond)
		
		if len(center)>0 { result = plan.NewFilter(expression.JoinAnd(center...),result) }
		
		return result,true
	case *plan.RightJoin:
		leftsz := len(v.Left.Schema())
		left,right,center := splitLeftRight(parts,splitAt(leftsz))
		center = append(right,center...)
		
		nleft  = v.Left
		
		if len(left)>0 { nleft = plan.NewFilter(expression.JoinAnd(left...),nleft) }
		
		result = plan.NewRightJoin(nleft,v.Right,v.Cond)
		
		if len(center)>0 { result = plan.NewFilter(expression.JoinAnd(center...),result) }
		
		return result,true
	case *plan.LeftJoin:
		leftsz := len(v.Left.Schema())
		left,right,center := splitLeftRight(parts,splitAt(leftsz))
		for i := range right {
			right[i],err = legacy.TransformUpExpr(right[i],shiftRight(leftsz))
			if err!=nil { panic(err) }
		}
		
		center = append(center,left...)
		
		nright = v.Right
		
		if len(right)>0 { nright = plan.NewFilter(expression.JoinAnd(right...),nright) }
		
		result = plan.NewLeftJoin(v.Left,nright,v.Cond)
		
		if len(center)>0 { result = plan.NewFilter(expression.JoinAnd(center...),result) }
		
		return result,true
	//case *plan.TableAlias:
	//	nx,err := filter.Expression.TransformUp(shiftRename(v.Child.Schema()))
	//	if err!=nil { return nil,false }
	//	return plan.NewTableAlias(v.Name(),plan.NewFilter(nx,v.Child)),true
	}
	
	return nil,false
}

func removeCruft(node sql.Node) (sql.Node, bool) {
	switch v := node.(type) {
	case *plan.TableAlias: return v.Child,true
	}
	return nil,false
}



/*
A Push-Down rule, that can!
*/
func Pushdown(c *sql.Context, a *analyzer.Analyzer, n sql.Node) (sql.Node, error) {
	changed := false
	
	txform := func(old sql.Node) (sql.Node, error) {
		node,ok := removeCruft(old)
		if ok { changed,old = true,node }
		
		node,ok = pushdown(old)
		if ok { changed,old = true,node }
		
		return old,nil
	}
	
	nn,err := legacy.TransformDownNode(n,txform)
	
	if err!=nil { return nil,err }
	if !changed { return n,nil }
	/*
	for i := 1; changed && i<8 ; i++ {
		changed = false
		nn,err = legacy.TransformUpNode(n,txform)
		if err!=nil { return nil,err }
	}
	*/
	
	return nn,nil
}


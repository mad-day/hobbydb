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

import "fmt"
import "github.com/src-d/go-mysql-server/sql"
import "github.com/src-d/go-mysql-server/sql/analyzer"
import "github.com/src-d/go-mysql-server/sql/expression"
import "github.com/src-d/go-mysql-server/sql/plan"

import "reflect"
import "github.com/mad-day/hobbydb/modern/eplan"

func printer(title string)func(*sql.Context, *analyzer.Analyzer, sql.Node) (sql.Node, error) {
	return func(c *sql.Context, a *analyzer.Analyzer, n sql.Node) (sql.Node, error) {
		fmt.Printf("-->%s\n%v\n",title,n)
		return n,nil
	}
}

//func(node sql.Node) (sql.Node, error) {}

func debugMe(node sql.Node) (sql.Node, error) {
	fmt.Println(reflect.TypeOf(node))
	fmt.Println(node)
	fmt.Println()
	return node,nil
}

func isGetField(expr sql.Expression) int {
	if gf,ok := expr.(*expression.GetField); ok {
		return gf.Index()
	}
	return -1
}

func isRightGetField(i int) eplan.TableFieldChecker {
	return func(expr sql.Expression) int {
		if gf,ok := expr.(*expression.GetField); ok {
			if gf.Index()<i { return -1 }
			return gf.Index()-i
		}
		return -1
	}
}

func resolveFilters(node sql.Node) (sql.Node, bool) {
	filter,ok := node.(*plan.Filter)
	if !ok { return nil,false }
	
	var parts []sql.Expression
	splitAnd(&parts,filter.Expression)
	
	if nf,ok := filter.Child.(*plan.Filter); ok {
		filter = nf
		splitAnd(&parts,filter.Expression)
	}
	
	j := 0
	
	var result sql.Node
	
	switch v := filter.Child.(type) {
	case *plan.ResolvedTable:
		trf := make(eplan.TableRowFilter,len(v.Schema()))
		for _,part := range parts {
			if !trf.Add(eplan.ConvertFilterExpr(part,isGetField)) {
				parts[j] = part
				j++
			}
		}
		result = &eplan.TableScan{Table:v.Table,RowFilter:trf}
		
		if j>0 {
			result = plan.NewFilter(expression.JoinAnd(parts[:j]...), result)
		}
		
		return result,true
	case *eplan.TableScan:
		trf := v.RowFilter.Clone()
		for _,part := range parts {
			if !trf.Add(eplan.ConvertFilterExpr(part,isGetField)) {
				parts[j] = part
				j++
			}
		}
		nnl := *v
		nnl.RowFilter = trf
		nnl.LookupHint = nil
		result = &nnl
		
		if j>0 {
			result = plan.NewFilter(expression.JoinAnd(parts[:j]...), result)
		}
		
		return result,true
	case *eplan.NestedLoopScan:
		
		// We must not push filters through right outer join.
		if v.RightOuter { return nil,false }
		
		trf := v.RowFilter.Clone()
		igf := isRightGetField(len(v.Left.Schema()))
		for _,part := range parts {
			if !trf.Add(eplan.ConvertFilterExpr(part,igf)) {
				parts[j] = part
				j++
			}
		}
		nnl := *v
		nnl.RowFilter = trf
		nnl.LookupHint = nil
		result = &nnl
		
		if j>0 {
			result = plan.NewFilter(expression.JoinAnd(parts[:j]...), result)
		}
		
		return result,true
	}
	
	return nil,false
}
func resolveJoins(node sql.Node) (sql.Node, bool) {
	var left,right,result sql.Node
	var parts []sql.Expression
	
	rightOuter := false
	isLOJ := false
	
	switch v := node.(type) {
	case *plan.CrossJoin: left,right = v.Left,v.Right
	case *plan.InnerJoin:
		left,right = v.Left,v.Right
		splitAnd(&parts,v.Cond)
	case *plan.RightJoin:
		left,right = v.Left,v.Right
		splitAnd(&parts,v.Cond)
		rightOuter = true
	//case *plan.LeftJoin:
	//	left,right = v.Left,v.Right
	//	splitAnd(&parts,v.Cond)
	//	isLOJ = true
	default: return nil,false
	}
	
	if isLOJ { panic("...") }
	
	filter,_ := right.(*plan.Filter)
	if filter!=nil { right = filter.Child }
	
	var trf eplan.TableRowFilter
	var tab sql.Table
	
	switch v := right.(type) {
	case *plan.ResolvedTable:
		trf = make(eplan.TableRowFilter,len(right.Schema()))
		tab = v.Table
	case *eplan.TableScan:
		trf = v.RowFilter.Clone()
		tab = v.Table
	default: return nil,false
	}
	
	j := 0
	
	leftlen := len(left.Schema())
	
	ffield := isRightGetField(leftlen)
	for _,part := range parts {
		if !trf.Add(eplan.ConvertFilterExpr(part,ffield)) {
			parts[j] = part
			j++
		}
	}
	
	result = &eplan.NestedLoopScan{
		Table      : tab,
		Left       : left,
		RightOuter : rightOuter,
		RowFilter  : trf,
	}
	
	
	if filter!=nil {
		expr,err := filter.Expression.TransformUp(shiftLeft(leftlen))
		if err!=nil { panic(err) }
		parts = append(parts[:j],expr)
		result = plan.NewFilter(expression.JoinAnd(parts...), result)
	} else {
		if j>0 {
			result = plan.NewFilter(expression.JoinAnd(parts[:j]...), result)
		}
	}
	
	return result,true
}


func CreateLookupNodes(c *sql.Context, a *analyzer.Analyzer, n sql.Node) (sql.Node, error) {
	//n.TransformUp(debugMe)
	changed := false
	
	//i := 0
	
	//fmt.Print("\n\n\n=======\n")
	
	nn,err := n.TransformUp(func(node sql.Node) (sql.Node, error){
		//i++
		//fmt.Println(i,":",reflect.TypeOf(node))
		
		nn,ok := resolveFilters(node)
		if ok { changed = true; return nn,nil }
		
		nn,ok = resolveJoins(node)
		if ok { changed = true; return nn,nil }
		
		return node,nil
	})
	//fmt.Print("\n\n\n-------",err,changed,"\n",nn,"\n\n\n")
	if err!=nil { return nil,err }
	
	if changed { return nn,err }
	
	return n,nil
}


func OptimizeLookupNodes(c *sql.Context, a *analyzer.Analyzer, n sql.Node) (sql.Node, error) {
	changed := false
	
	nn,err := n.TransformUp(func(node sql.Node) (sql.Node, error){
		var tab sql.Table
		var filter eplan.TableRowFilter
		switch v := node.(type) {
		case *eplan.TableScan: tab,filter = v.Table,v.RowFilter
		case *eplan.NestedLoopScan: tab,filter = v.Table,v.RowFilter
		default: return node,nil
		}
		flhg,ok := tab.(eplan.FastLookupHintGenerator)
		if !ok { return node,nil }
		
		hint,err := flhg.GenerateLookupHint(c,filter)
		if err!=nil { return nil,err }
		changed = true
		
		switch v := node.(type) {
		case *eplan.TableScan:
			t := *v
			t.LookupHint = hint
			return &t,nil
		case *eplan.NestedLoopScan:
			t := *v
			t.LookupHint = hint
			return &t,nil
		}
		panic("...")
	})
	if err!=nil { return nil,err }
	
	if changed { return nn,err }
	
	return n,nil
}


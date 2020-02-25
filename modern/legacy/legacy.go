// Freely Redistributable.


package legacy

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
)

// Copied from "github.com/src-d/go-mysql-server/sql"
// 
// TransformNodeFunc is a function that given a node will return that node
// as is or transformed along with an error, if any.
type TransformNodeFunc func(sql.Node) (sql.Node, error)

// Copied from "github.com/src-d/go-mysql-server/sql"
// 
// TransformExprFunc is a function that given an expression will return that
// expression as is or transformed along with an error, if any.
type TransformExprFunc func(sql.Expression) (sql.Expression, error)


func TransformUpNode(on sql.Node,f TransformNodeFunc) (nn sql.Node,err error) {
	oldSub := on.Children()
	newSub := make([]sql.Node,len(oldSub))
	for i,cld := range oldSub {
		newSub[i],err = TransformUpNode(cld,f)
		if err!=nil { return }
	}
	on,err = on.WithChildren(newSub...)
	if err!=nil { return }
	return f(on)
}
func TransformUpExpr(on sql.Expression,f TransformExprFunc) (nn sql.Expression,err error) {
	oldSub := on.Children()
	newSub := make([]sql.Expression,len(oldSub))
	for i,cld := range oldSub {
		newSub[i],err = TransformUpExpr(cld,f)
		if err!=nil { return }
	}
	on,err = on.WithChildren(newSub...)
	if err!=nil { return }
	return f(on)
}

func TransformDownNode(on sql.Node,f TransformNodeFunc) (nn sql.Node,err error) {
	on,err = f(on);
	if err!=nil { return }
	
	oldSub := on.Children()
	newSub := make([]sql.Node,len(oldSub))
	for i,cld := range oldSub {
		newSub[i],err = TransformDownNode(cld,f)
		if err!=nil { return }
	}
	nn,err = on.WithChildren(newSub...)
	return
}

func unpackSubquery(ref sql.Node) sql.Node {
	for {
		switch ref.(type) {
		case *plan.SubqueryAlias,*plan.QueryProcess:
			ref = ref.Children()[0]
		default: return ref
		}
	}
	return ref
}
func replaceSubquery(ref, alt, nn sql.Node) sql.Node {
	switch v := ref.(type) {
	case *plan.SubqueryAlias:
		return plan.NewSubqueryAlias(v.Name(),replaceSubquery(v.Child,alt,nn))
	case *plan.QueryProcess:
		return plan.NewQueryProcess(replaceSubquery(v.Child,alt,nn),v.Notify)
	}
	if ref!=alt { panic("invalid state") }
	return nn
}

func TransformUpNodeWithSubqueries(on sql.Node,f TransformNodeFunc) (nn sql.Node,err error) {
	if alt := unpackSubquery(on) ; alt!=on {
		nn,err = TransformUpNodeWithSubqueries(alt,f)
		if err==nil { nn = replaceSubquery(on,alt,nn) }
		return
	}
	
	oldSub := on.Children()
	newSub := make([]sql.Node,len(oldSub))
	for i,cld := range oldSub {
		newSub[i],err = TransformUpNodeWithSubqueries(cld,f)
		if err!=nil { return }
	}
	on,err = on.WithChildren(newSub...)
	if err!=nil { return }
	return f(on)
}


// #

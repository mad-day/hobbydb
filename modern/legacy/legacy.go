// Freely Redistributable.


package legacy

import (
	"github.com/src-d/go-mysql-server/sql"
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



// #

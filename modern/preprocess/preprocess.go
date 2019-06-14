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
Preprocessing Stuff.
*/
package preprocess

import "strings"
import "errors"
import "context"
import "github.com/src-d/go-mysql-server/sql"
import "github.com/src-d/go-mysql-server/sql/parse"
import "vitess.io/vitess/go/vt/sqlparser"

type sqlKey struct{
	name string
}

var insertOperation interface{} = sqlKey{"insert.op"}

func InsertKey() interface{} { return insertOperation }

func InsertValue(ctx *sql.Context) string {
	str,_ := ctx.Value(insertOperation).(string)
	return str
}

var (
	ECantUpdatePK = parse.ErrUnsupportedFeature.New("Can't update primary key")
	EUpdateTooManyTables = parse.ErrUnsupportedFeature.New("Update: too many tables")
	
	EUpdateSubquery = parse.ErrUnsupportedSyntax.New("Update on Subquery")
	
	//ENotFound = errors.New("ColumnNotFound")
)
var (
	eIgnore = errors.New("---------Ignore me please----------")
)

/*
An sql.Table with a primary key.
*/
type PrimaryKeyTable interface {
	// Returns true, if the column is a primary key.
	IsPrimaryKey(column string) bool
}

type mockPKT struct{}
func (mockPKT) IsPrimaryKey(column string) bool { return true }

func PreprocessSimple(ctx *sql.Context, cat *sql.Catalog, query string) (nc *sql.Context,nq string,err error){
	if s,e := sqlparser.Parse(query); e==nil {
		nc,s,err = Preprocess(ctx,cat,s)
		if s!=nil { nq = sqlparser.String(s) }
		return
	}
	
	
	iop := "insert"
	
	/*
	nq = query
	
	if s,e := sqlparser.Parse(query); e==nil {
		switch v := s.(type) {
		case *sqlparser.Insert: iop = v.Action
		case *sqlparser.Update:
			s,err = rewriteUpdateTable(cat,v)
			if err!=nil { return }
			nq = sqlparser.String(s)
			iop = "update"
		}
	}
	*/
	
	{
		r := ctx.Context
		r = context.WithValue(r,insertOperation,iop)
		nc = ctx.WithContext(r)
	}
	return
}

/*
Finds unsupported Queries and replaces them with a safe replacement.
*/
func Preprocess(ctx *sql.Context, cat *sql.Catalog, query sqlparser.Statement) (nc *sql.Context,nq sqlparser.Statement,err error) {
	iop := "insert"
	
	nq = query
	{
		switch v := query.(type) {
		case *sqlparser.Insert: iop = v.Action
		case *sqlparser.Update:
			nq,err = rewriteUpdateTable(cat,v)
			if err==eIgnore { nq,err = query,nil; break }
			if err!=nil { return }
			iop = "update"
		case *sqlparser.Delete:
			nq,err = rewriteDeleteTable(cat,v)
			if err==eIgnore { nq,err = query,nil; break }
			if err!=nil { return }
			iop = "delete"
		}
	}
	
	{
		r := ctx.Context
		r = context.WithValue(r,insertOperation,iop)
		nc = ctx.WithContext(r)
	}
	return
}

func findUpdateTable(cat *sql.Catalog, tabs sqlparser.TableExprs) (sql.Table,sqlparser.TableName,error) {
	var tab sqlparser.TableName
restart:
	if len(tabs)!=1 { return nil,tab,EUpdateTooManyTables }
	switch v := tabs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		switch v2 := v.Expr.(type) {
		case *sqlparser.Subquery: return nil,tab,EUpdateSubquery
		case sqlparser.TableName: tab = v2
		default: return nil,tab,eIgnore
		}
	case *sqlparser.ParenTableExpr:
		tabs = v.Exprs
		goto restart
	default:
		return nil,tab,EUpdateTooManyTables
	}
	
	//
	db := tab.Qualifier.String()
	if db=="" { db = cat.CurrentDatabase() }
	dbi,err := cat.Database(db)
	if err!=nil { return nil,tab,err }
	
	n := tab.Name.String()
	
	ti,ok := dbi.Tables()[n]
	if !ok { return nil,tab,eIgnore }
	
	return ti,tab,nil
}

func rewriteUpdateTable(cat *sql.Catalog,u *sqlparser.Update) (sqlparser.Statement,error) {
	tab,nam,err := findUpdateTable(cat,u.TableExprs)
	
	if err!=nil { return nil,err }
	
	columns := tab.Schema()
	columns_m := make(map[string]int)
	
	columns_pk := make([]bool,len(columns))
	columns_repl := make(sqlparser.SelectExprs,len(columns))
	columns_nms := make(sqlparser.Columns,len(columns))
	
	pkt,_ := tab.(PrimaryKeyTable)
	if pkt==nil { pkt = mockPKT{} }
	
	for i,col := range columns {
		columns_m[strings.ToLower(col.Name)] = i
		columns_pk[i] = pkt.IsPrimaryKey(col.Name)
		columns_nms[i] = sqlparser.NewColIdent(col.Name)
	}
	
	for _,ue := range u.Exprs {
		cn := strings.ToLower(ue.Name.Name.String())
		i,ok := columns_m[cn]
		if !ok {
			return nil,errors.New("Column not found: "+cn)
		}
		if columns_pk[i] { return nil,ECantUpdatePK }
		columns_repl[i] = &sqlparser.AliasedExpr{Expr:ue.Expr,As:columns_nms[i]}
	}
	for i,ci := range columns_nms {
		if columns_repl[i]!=nil { continue }
		columns_repl[i] = &sqlparser.AliasedExpr{ Expr:&sqlparser.ColName{Name:ci,Qualifier:nam}, As: ci}
	}
	
	sel := &sqlparser.Select{
		SelectExprs: columns_repl,
		Comments: u.Comments,
		From    : u.TableExprs,
		Where   : u.Where,
		OrderBy : u.OrderBy,
		Limit   : u.Limit,
	}
	
	ins := &sqlparser.Insert{
		Action: sqlparser.InsertStr,
		Comments: u.Comments,
		Table: nam,
		Columns: columns_nms,
		Rows: sel,
		OnDup: nil,
	}
	
	return ins,nil
}

func findDeleteTable(cat *sql.Catalog, tabs sqlparser.TableNames) (sql.Table,sqlparser.TableName,error) {
	var tab sqlparser.TableName
	if len(tabs)!=1 { return nil,tab,EUpdateTooManyTables }
	
	tab = tabs[0]
	
	//
	db := tab.Qualifier.String()
	if db=="" { db = cat.CurrentDatabase() }
	dbi,err := cat.Database(db)
	if err!=nil { return nil,tab,err }
	
	n := tab.Name.String()
	
	ti,ok := dbi.Tables()[n]
	if !ok { return nil,tab,eIgnore }
	
	return ti,tab,nil
}

func rewriteDeleteTable(cat *sql.Catalog,u *sqlparser.Delete) (sqlparser.Statement,error) {
	var tab sql.Table
	var nam sqlparser.TableName
	var err error
	if len(u.Targets)==0 {
		tab,nam,err = findUpdateTable(cat,u.TableExprs)
	} else {
		tab,nam,err = findDeleteTable(cat,u.Targets)
	}
	
	if err!=nil { return nil,err }
	
	columns := tab.Schema()
	columns_m := make(map[string]int)
	
	columns_pk := make([]bool,len(columns))
	columns_repl := make(sqlparser.SelectExprs,len(columns))
	columns_nms := make(sqlparser.Columns,len(columns))
	
	pkt,_ := tab.(PrimaryKeyTable)
	if pkt==nil { pkt = mockPKT{} }
	
	for i,col := range columns {
		columns_m[strings.ToLower(col.Name)] = i
		columns_pk[i] = pkt.IsPrimaryKey(col.Name)
		columns_nms[i] = sqlparser.NewColIdent(col.Name)
	}
	
	
	
	for i,ci := range columns_nms {
		if columns_repl[i]!=nil { continue }
		columns_repl[i] = &sqlparser.AliasedExpr{ Expr:&sqlparser.ColName{Name:ci,Qualifier:nam}, As: ci}
	}
	
	sel := &sqlparser.Select{
		SelectExprs: columns_repl,
		Comments: u.Comments,
		From    : u.TableExprs,
		Where   : u.Where,
		OrderBy : u.OrderBy,
		Limit   : u.Limit,
	}
	
	ins := &sqlparser.Insert{
		Action: sqlparser.InsertStr,
		Comments: u.Comments,
		Table: nam,
		Columns: columns_nms,
		Rows: sel,
		OnDup: nil,
	}
	
	return ins,nil
}


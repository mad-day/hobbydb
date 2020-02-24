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



package mmapv2

import "github.com/src-d/go-mysql-server/sql"
import "github.com/mad-day/hobbydb/modern/containers"
import "github.com/mad-day/hobbydb/modern/eplan"
//import "github.com/mad-day/hobbydb/modern/preprocess"

type xte struct {
	orEq bool
	data interface{}
}




type SqlTable struct {
	Table   *Table
	SqlName string
	Data    sql.Schema
}
func (s *SqlTable) IsPrimaryKey(column string) bool { return s.Data[0].Name==column }
func (s *SqlTable) Name() string { return s.SqlName }
func (s *SqlTable) String() string { return "MMAPv2{"+s.SqlName+"}" }
func (s *SqlTable) Schema() sql.Schema { return s.Data }
func (s *SqlTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &containers.PartitionIter{ containers.Partition("self") },nil
}
func (s *SqlTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	bat,_ := ctx.Value(s.Table.env).(*BAT)
	raw,err := s.Table.RawIterator(bat)
	if err!=nil { return nil,err }
	return raw2row{raw},nil
}
func (s *SqlTable) GetSubsetTable(ctx *sql.Context, hint interface{}, filter eplan.TableRowFilter) (sql.Table, error) {
	return &lookupSqlTable{s,filter},nil
}
type lookupSqlTable struct {
	*SqlTable
	filter eplan.TableRowFilter
}
func (s *lookupSqlTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	bat,_ := ctx.Value(s.Table.env).(*BAT)
	
	lkup,err := s.Table.Lookup(bat)
	if err!=nil { return nil,err }
	
	for i,cf := range s.filter {
		for _,ccf := range cf {
			var gte []xte
			var lte []xte
			switch ccf.Op {
			//case eplan.TF_True,eplan.TF_False:
			case eplan.TF_Eq:
				lkup.Equals(uint32(i),ccf.Arg[0].GetValue())
			case eplan.TF_Gt,eplan.TF_Ge:
				v := ccf.Arg[0].GetValue()
				if sortable(v) { gte = append(gte,xte{ccf.Op==eplan.TF_Ge,v}) }
			case eplan.TF_Lt,eplan.TF_Le:
				v := ccf.Arg[0].GetValue()
				if sortable(v) { lte = append(lte,xte{ccf.Op==eplan.TF_Le,v}) }
			case eplan.TF_In:
				for _,arg := range ccf.Arg {
					lkup.OrEquals(uint32(i),arg.GetValue())
				}
				lkup.EndOr()
			}
			if len(gte)>0 && len(lte)>0 {
				// ...
				lkup.Range(uint32(i),gte[0].data,lte[0].data,gte[0].orEq,lte[0].orEq)
			} else if len(gte)>0 {
				for _,v := range gte {
					lkup.GreaterThan(uint32(i),v.data,v.orEq)
				}
			}
		}
	}
	
	raw,err := lkup.RawIterator()
	if err!=nil { return nil,err }
	
	return raw2row{raw},nil
}

func (s *SqlTable) Insert(ctx *sql.Context, row sql.Row) error {
	bat,_ := ctx.Value(s.Table.env).(*BAT)
	if bat==nil {
		nb,err := s.Table.env.Begin()
		if err!=nil { return err }
		defer nb.Commit()
		bat = nb
	}
	/*
	switch preprocess.InsertValue(ctx) {
	case "","insert": break
	case "delete": return s.Table.RawDelete(bat,row)
	case "replace","update": return s.Table.RawUpsert(bat,row)
	}
	*/
	return s.Table.RawInsert(bat,row)
}
func (s *SqlTable) Delete(ctx *sql.Context, row sql.Row) error {
	bat,_ := ctx.Value(s.Table.env).(*BAT)
	if bat==nil {
		nb,err := s.Table.env.Begin()
		if err!=nil { return err }
		defer nb.Commit()
		bat = nb
	}
	return s.Table.RawDelete(bat,row)
}
func (s *SqlTable) Update(ctx *sql.Context, oldr, newr sql.Row) error {
	bat,_ := ctx.Value(s.Table.env).(*BAT)
	if bat==nil {
		nb,err := s.Table.env.Begin()
		if err!=nil { return err }
		defer nb.Commit()
		bat = nb
	}
	return s.Table.RawUpdate(bat,oldr,newr)
}


var _ sql.Table = (*SqlTable)(nil)

type raw2row struct{
	TableIterator
}
func (r raw2row) Next() (sql.Row, error) {
	return r.RawNext()
}


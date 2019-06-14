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
	"vitess.io/vitess/go/vt/sqlparser"
	
	"github.com/mad-day/hobbydb/modern/schema"
	"regexp"
	"fmt"
)

var (
	ct_format = regexp.MustCompile(`format=([a-zA-Z]+)`)
)

func NewTableMetadata(llmd *schema.TableMasterMetadata) (md *TableMetadata, _ error) {
	md = &TableMetadata{Master:*llmd}
	md.Master.Options = ""
	
	if sm := ct_format.FindStringSubmatch(llmd.Options); len(sm)!=0 {
		md.Format = sm[1]
	}
	
	md.PrimaryKey = make([]int,len(llmd.PKey))
	for i,name := range llmd.PKey {
		var ok bool
		md.PrimaryKey[i],ok = llmd.Schema.FindIndex(name)
		if !ok { return nil,fmt.Errorf("Public key column not found: %q",name) }
	}
	
	return
}

func TableMetadataFromSql(sql sqlparser.Statement) (md *TableMetadata, _ error) {
	ddl,_ := sql.(*sqlparser.DDL)
	if ddl==nil { return nil,fmt.Errorf("Not DDL %v",ddl) }
	
	llmd,err := schema.ParseTableMasterMetadata(ddl)
	
	if err!=nil { return nil,err }
	
	return NewTableMetadata(llmd)
}

func TableMetadataFromSqlText(sql string) (md *TableMetadata, _ error) {
	stmt,err := sqlparser.Parse(sql)
	if err!=nil { return nil,err }
	
	return TableMetadataFromSql(stmt)
}



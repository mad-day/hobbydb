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



/*
To be implemented by sql.Table instances.
*/
type FastLookupHintGenerator interface{
	// Computes a 'hint', that could be supplied as the 'hint' parameter of
	// the .GetSubsetTable() method of the same instance.
	GenerateLookupHint(ctx *sql.Context,filter TableRowFilter) (interface{},error)
}

/*
To be implemented by sql.Table instances.
*/
type FastLookupTable interface{
	// Returns a View of the Table, that is
	//   1. a subset of all tuples of the table
	//   2. a superset of all tuples of the table, that will match the supplied filter
	//
	// The parameter 'hint' SHALL be nil in most cases.
	//
	// If the table also implements the FastLookupHintGenerator-interface,
	// the query optimizer MAY call the .GenerateLookupHint()-method to obtain
	// a 'hint'-value, which can be supplied through this parameter.
	GetSubsetTable(ctx *sql.Context,hint interface{}, filter TableRowFilter) (sql.Table,error)
}


/*
An interface, that can be implemented by Lookup-Hints.

This interface helps the Table-provider to expose Query-Plan informations for a given Lookup-Hint.
*/
type FastLookupHintExplainable interface{
	ExplainPlan() string
}

func FastLookupHintExplain(i interface{}) (string,bool) {
	if i==nil { return "",false }
	if exp,ok := i.(FastLookupHintExplainable); ok { return exp.ExplainPlan(),true }
	return "<OPTIMIZED>",true
}

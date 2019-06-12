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
Container object Implementations (like I-Database).
*/
package containers

import "github.com/src-d/go-mysql-server/sql"
import "io"

var eof = io.EOF

func clonetbs(t map[string]sql.Table) (n map[string]sql.Table) {
	n = make(map[string]sql.Table)
	for k,v := range t { n[k] = v }
	return
}
type Database struct {
	name string
	tables map[string]sql.Table
}
func (d *Database) Add(tab sql.Table) {
	n := clonetbs(d.tables)
	n[tab.Name()] = tab
	d.tables = n
}
func (d *Database) SetName(name string) { d.name = name }

func (d *Database) Name() string { return d.name }
func (d *Database) Tables() map[string]sql.Table { return d.tables }


var _ sql.Database = (*Database)(nil)

type PartitionIter []sql.Partition
var _ sql.PartitionIter = (*PartitionIter)(nil)
func (p *PartitionIter) Close() error { return nil }
func (p *PartitionIter) Next() (sql.Partition, error) {
	l := *p
	if len(l)==0 { return nil,io.EOF }
	*p = l[1:]
	
	return l[0],nil
}

type Partition string
func (p Partition) Key() []byte { return []byte(p) }

type PrimaryKeyLookup struct{
	Index string
	Key   []byte
}
var _ sql.IndexLookup = (*PrimaryKeyLookup)(nil)
func (p *PrimaryKeyLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return &IndexValueIter{p.Key},nil
}
func (p *PrimaryKeyLookup) Indexes() []string { return []string{p.Index} }

type IndexValueIter [][]byte
func (p *IndexValueIter) Close() error { return nil }
func (p *IndexValueIter) Next() ([]byte, error) {
	l := *p
	if len(l)==0 { return nil,io.EOF }
	*p = l[1:]
	
	return l[0],nil
}


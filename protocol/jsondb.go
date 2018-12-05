/*
Copyright (c) 2018 Simon Schmidt

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


package protocol

import (
	"regexp"
	"net/textproto"
	"github.com/mad-day/hobbydb/lstore"
	"fmt"
	"encoding/json"
	jsonpatch "github.com/evanphx/json-patch"
)
/*
 TODO use:
   maybe "gopkg.in/src-d/go-vitess.v1"
*/

var eBYE = fmt.Errorf("bye")

var cmd = regexp.MustCompile(`^([a-z0-9_]+)\s+([a-z0-9_]+)(?:\s+(.*))?$`)
var cmd2 = regexp.MustCompile(`^([a-z0-9_]+)`)


type cctx struct{
	C *textproto.Conn
	DS lstore.UDBM
	TX lstore.UDB
}

func normalizeJson(i []byte) (r []byte,err error) {
	var mykey interface{}
	err = json.Unmarshal(i,&mykey)
	if err!=nil { return }
	r,err = json.Marshal(mykey)
	return
}

func (c *cctx) perform() error {
	var args [3][]byte
	rl,err := c.C.ReadLineBytes()
	if sm := cmd.FindSubmatchIndex(rl);len(sm)!=0 {
		args[0] = rl[sm[2]:sm[3]]
		args[1] = rl[sm[4]:sm[5]]
		if sm[6]>=0 { args[2] = rl[sm[6]:sm[7]] }
	} else if sm := cmd2.FindSubmatchIndex(rl) ; len(sm)!=0 {
		args[0] = rl[sm[2]:sm[3]]
	} else {
		return c.C.PrintfLine("999 Invalid command")
	}
	//c.C.PrintfLine("%q %q %q",string(args[0]),string(args[1]),string(args[2]))
	switch string(args[0]){
	case "quit":
		c.C.PrintfLine("250 bye")
		return eBYE
	case "tx_full","tx_batch","tx_auto","tx_blind","tx_read":
		if c.TX!=nil { return c.C.PrintfLine("981 Active transaction") }
		var ri lstore.ReadIso
		var wi lstore.WriteIso
		switch string(args[0]) {
		case "tx_full": wi = lstore.WRITE_CHECKED
		case "tx_batch": wi = lstore.WRITE_COMMIT
		case "tx_auto": wi = lstore.WRITE_INSTANT_ATOMIC
		case "tx_blind": wi = lstore.WRITE_INSTANT
		case "tx_read": wi = lstore.WRITE_DISABLED
		}
		switch string(args[1]) {
		case "snapshot": ri = lstore.READ_SNAPSHOT
		case "repeatable": ri = lstore.READ_REPEATABLE
		case "any": ri = lstore.READ_ANY
		default: if c.TX==nil { return c.C.PrintfLine("990 unknown isolation level: %v",string(args[1])) }
		}
		c.TX = c.DS.StartTx(ri,wi)
		return c.C.PrintfLine("200 OK")
	case "commit":
		if c.TX==nil { return c.C.PrintfLine("980 No active transaction") }
		err = c.TX.Commit()
		c.TX = nil
		if err!=nil { return c.C.PrintfLine("710 Abort: %v",err) }
		return c.C.PrintfLine("200 OK")
	case "rollback":
		if c.TX==nil { return c.C.PrintfLine("980 No active transaction") }
		c.TX.Discard()
		c.TX = nil
		return c.C.PrintfLine("200 OK")
	default:
		if c.TX==nil { return c.C.PrintfLine("980 No active transaction") }
	}
	var u lstore.UTable
	var errt1 error
	u,errt1 = c.TX.UTable("json_"+string(args[1]))
	var mykey,myup,myvalue []byte
	
	switch string(args[0]){
	case "get":
		if errt1!=nil { return c.C.PrintfLine("800 IO Error: %v",err) }
		mykey,err = normalizeJson(args[2])
		if err!=nil { return c.C.PrintfLine("901 Invalid Key: %v",err) }
		err = c.C.PrintfLine("290 content follows")
		if err!=nil { return err }
		dw := c.C.DotWriter()
		defer dw.Close()
		_,err = dw.Write(u.Read(mykey))
		return err
	case "put","delete","merge","patch":
		if string(args[0])=="delete" {
			myup = nil
		} else {
			myup,err = c.C.ReadDotBytes()
			if err!=nil { return err }
		}
		mykey,err = normalizeJson(args[2])
		if err!=nil { return c.C.PrintfLine("901 Invalid Key: %v",err) }
		if errt1!=nil { return c.C.PrintfLine("800 IO Error: %v",err) }
		switch string(args[0]) {
		case "merge","patch":
			myvalue = u.Read(mykey)
			if len(myvalue)==0 { myvalue=[]byte("{}") }
		}
		switch string(args[0]) {
		case "merge":
			myup,err = jsonpatch.MergePatch(myvalue,myup)
			if err!=nil { return c.C.PrintfLine("998 Invalid merge patch: %v",err) }
		case "patch":
			patch, err := jsonpatch.DecodePatch(myup)
			if err!=nil { return c.C.PrintfLine("997 Invalid JSON patch: %v",err) }
			myup, err = patch.Apply(myvalue)
			if err!=nil { return c.C.PrintfLine("850 Corrupted JSON in db: %v",err) }
		}
		err = u.Write(mykey,myup)
		if err!=nil { return c.C.PrintfLine("700 write op %s: %v",args[0],err) }
		/*-----------------------------------------------------------------------------*/
		return c.C.PrintfLine("201 updated")
	case "list":
		iter := u.Iter()
		defer iter.Release()
		err = c.C.PrintfLine("202 list collection")
		if err!=nil { return err }
		for iter.Next() {
			c.C.PrintfLine("> %s",iter.Key())
			dw := c.C.DotWriter()
			_,err = dw.Write(iter.Value())
			if err!=nil { return err }
			err = dw.Close()
			if err!=nil { return err }
		}
		return c.C.PrintfLine("!")
	}
	return c.C.PrintfLine("996 unknown command %s",args[0])
	return nil
}

func Perform(l lstore.UDBM,c *textproto.Conn) {
	cc := &cctx{C:c,DS:l}
	for {
		err := cc.perform()
		if err!=nil { return }
	}
}


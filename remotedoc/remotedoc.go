/*
Copyright (c) 2020 Simon Schmidt

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



package remotedoc

import (
	"bufio"
	"sync"
	"net/textproto"
	"github.com/vmihailenco/msgpack"
)

type Reader struct {
	*textproto.Reader
	msgpack.Decoder
	wg sync.WaitGroup
}
func NewReader(br *bufio.Reader) *Reader {
	r := new(Reader)
	r.Reader = textproto.NewReader(br)
	r.Decoder.Reset(br)
	r.Decoder.UseDecodeInterfaceLoose(true)
	return r
}

type Writer struct {
	*textproto.Writer
	msgpack.Encoder
}

func NewWriter(bw *bufio.Writer) *Writer {
	r := new(Writer)
	r.Writer = textproto.NewWriter(bw)
	r.Encoder.Reset(bw)
	r.Encoder.UseCompactEncoding(true)
	return r
}
func (w *Writer) WriteCodeLine(code int, message string) error {
	return w.PrintfLine("%d %s",code,message)
}
func (w *Writer) WriteMIMEHeader(hdr textproto.MIMEHeader) (err error) {
	for k,a := range hdr {
		for _,v := range a {
			if err!=nil { return }
			err = w.PrintfLine("%s: %s",k,v)
		}
	}
	return
}



// #

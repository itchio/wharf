package wharf

import (
	"encoding/gob"
	"io"
	"io/ioutil"

	"github.com/itchio/wharf/counter"

	"golang.org/x/crypto/ssh"
	"gopkg.in/kothar/brotli-go.v0/dec"
	"gopkg.in/kothar/brotli-go.v0/enc"
)

type Channel struct {
	ch     ssh.Channel
	closed bool

	bw *enc.BrotliWriter
	br *dec.BrotliReader

	wc *counter.CounterWriter

	genc *gob.Encoder
	gdec *gob.Decoder
}

func (ch *Channel) CloseWrite() error {
	// flush rest of brotli data
	err := ch.bw.Close()
	if err != nil {
		return err
	}

	// close write end of our channel
	err = ch.ch.CloseWrite()
	if err != nil {
		return err
	}

	return nil
}

func (ch *Channel) Abort() error {
	return ch.ch.Close()
}

// Close write end if not closed, blocks until peer has closed their end
func (ch *Channel) AwaitEOF() error {
	// read until EOF
	_, err := io.Copy(ioutil.Discard, ch.br)
	return err
}

func (ch *Channel) Send(graal interface{}) error {
	return ch.genc.Encode(&graal)
}

func (ch *Channel) Receive() (interface{}, error) {
	var graal interface{}
	err := ch.gdec.Decode(&graal)
	if err != nil {
		return nil, err
	}

	return graal, nil
}

func (c *Channel) BytesWritten() int64 {
	return c.wc.Count()
}

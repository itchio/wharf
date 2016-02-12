package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/golang/protobuf/proto"
)

type ReadContext struct {
	reader io.Reader
}

func NewReadContext(reader io.Reader) *ReadContext {
	return &ReadContext{reader}
}

func (r *ReadContext) Reader() io.Reader {
	return r.reader
}

func (r *ReadContext) ExpectMagic(magic int32) error {
	var readMagic int32
	err := binary.Read(r.reader, ENDIANNESS, &readMagic)
	if err != nil {
		return err
	}

	if magic != readMagic {
		return fmt.Errorf("expected magic %x, but read %x", magic, readMagic)
	}

	return nil
}

func (r *ReadContext) ReadMessage(msg proto.Message) error {
	var length uint32
	err := binary.Read(r.reader, ENDIANNESS, &length)
	if err != nil {
		return err
	}

	buf := make([]byte, length)

	_, err = io.ReadFull(r.reader, buf)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(buf, msg)
	if err != nil {
		return err
	}

	if DEBUG_WIRE {
		fmt.Printf(">> %s %+v\n", reflect.TypeOf(msg).Elem().Name(), msg)
	}

	return nil
}

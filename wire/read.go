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

	byteBuffer []byte
	msgBuf     []byte
}

func NewReadContext(reader io.Reader) *ReadContext {
	return &ReadContext{reader, make([]byte, 1), make([]byte, 32)}
}

func (r *ReadContext) ReadByte() (byte, error) {
	_, err := r.reader.Read(r.byteBuffer)
	if err != nil {
		return 0, err
	}

	return r.byteBuffer[0], nil
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
	length, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	if cap(r.msgBuf) < int(length) {
		r.msgBuf = make([]byte, length)
	}

	_, err = io.ReadFull(r.reader, r.msgBuf[:length])
	if err != nil {
		return err
	}

	err = proto.Unmarshal(r.msgBuf[:length], msg)
	if err != nil {
		return err
	}

	if DEBUG_WIRE {
		fmt.Printf(">> %s %+v\n", reflect.TypeOf(msg).Elem().Name(), msg)
	}

	return nil
}

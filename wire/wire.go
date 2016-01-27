package wire

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/golang/protobuf/proto"
)

var ENDIANNESS = binary.LittleEndian

const MSG_MAGIC = uint16(0xD3F1)

var (
	ErrInvalidMagic = errors.New("Invalid magic number")
)

type WriteContext struct {
	writer io.Writer
	buf    *proto.Buffer
}

func NewWriteContext(writer io.Writer) *WriteContext {
	buf := proto.NewBuffer(nil)
	return &WriteContext{writer, buf}
}

func (w *WriteContext) WriteMessage(msg proto.Message) error {
	err := binary.Write(w.writer, ENDIANNESS, uint16(MSG_MAGIC))
	if err != nil {
		return err
	}

	w.buf.Reset()
	w.buf.EncodeMessage(msg)

	_, err = w.writer.Write(w.buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

type ReadContext struct {
	reader io.Reader
	buf    *proto.Buffer
}

func NewReadContext(reader io.Reader) *ReadContext {
	buf := proto.NewBuffer(nil)
	return &ReadContext{reader, buf}
}

func (r *ReadContext) ReadMessage(msg proto.Message) error {
	var magic uint16
	err := binary.Read(r.reader, ENDIANNESS, &magic)
	if err != nil {
		return err
	}

	if magic != MSG_MAGIC {
		return ErrInvalidMagic
	}

	err = r.buf.DecodeMessage(msg)
	if err != nil {
		return err
	}

	return nil
}

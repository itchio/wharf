package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/golang/protobuf/proto"
)

type WriteContext struct {
	writer io.Writer
}

func NewWriteContext(writer io.Writer) *WriteContext {
	return &WriteContext{writer}
}

func (w *WriteContext) Writer() io.Writer {
	return w.writer
}

func (w *WriteContext) Close() error {
	if c, ok := w.writer.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func (w *WriteContext) WriteMagic(magic int32) error {
	return binary.Write(w.writer, ENDIANNESS, magic)
}

func (w *WriteContext) WriteMessage(msg proto.Message) error {
	if DEBUG_WIRE {
		fmt.Printf("<< %s %+v\n", reflect.TypeOf(msg).Elem().Name(), msg)
	}

	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	err = binary.Write(w.writer, ENDIANNESS, uint32(len(buf)))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

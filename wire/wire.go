package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/golang/protobuf/proto"
)

var ENDIANNESS = binary.LittleEndian

const MSG_MAGIC = uint16(0xD3F1)
const DEBUG_WIRE = false

type WriteContext struct {
	writer io.Writer
}

func NewWriteContext(writer io.Writer) *WriteContext {
	return &WriteContext{writer}
}

func (w *WriteContext) WriteMessage(msg proto.Message) error {
	if DEBUG_WIRE {
		fmt.Printf("<< %s %+v\n", reflect.TypeOf(msg).Elem().Name(), msg)
	}

	err := binary.Write(w.writer, ENDIANNESS, uint16(MSG_MAGIC))
	if err != nil {
		return err
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

type ReadContext struct {
	reader io.Reader
}

func NewReadContext(reader io.Reader) *ReadContext {
	return &ReadContext{reader}
}

func (r *ReadContext) ReadMessage(msg proto.Message) error {
	var magic uint16
	err := binary.Read(r.reader, ENDIANNESS, &magic)
	if err != nil {
		return fmt.Errorf("while reading magic: %s", err)
	}

	if magic != MSG_MAGIC {
		return fmt.Errorf("invalid magic number: %x", magic)
	}

	var length uint32
	err = binary.Read(r.reader, ENDIANNESS, &length)
	if err != nil {
		return fmt.Errorf("while reading length: %s", err)
	}

	buf := make([]byte, length)

	_, err = io.ReadFull(r.reader, buf)
	if err != nil {
		return fmt.Errorf("while readfull: %s", err)
	}

	err = proto.Unmarshal(buf, msg)
	if err != nil {
		return fmt.Errorf("while decoding message: %s", err)
	}

	if DEBUG_WIRE {
		fmt.Printf(">> %s %+v\n", reflect.TypeOf(msg).Elem().Name(), msg)
	}

	return nil
}

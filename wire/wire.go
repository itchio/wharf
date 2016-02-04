package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/golang/protobuf/proto"
)

var ENDIANNESS = binary.LittleEndian

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

	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	err = binary.Write(w.writer, ENDIANNESS, uint32(len(buf)))
	if err != nil {
		return err
	}

	fmt.Printf("%s %d\n", reflect.TypeOf(msg), len(buf))

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
	var length uint32
	err := binary.Read(r.reader, ENDIANNESS, &length)
	if err != nil {
		return fmt.Errorf("while reading length: %s", err)
	}

	buf := make([]byte, length)

	readBytes, err := io.ReadFull(r.reader, buf)
	if err != nil {
		return fmt.Errorf("while readfull: %s", err)
	}

	fmt.Printf("%s %d\n", reflect.TypeOf(msg), readBytes)

	err = proto.Unmarshal(buf, msg)
	if err != nil {
		return fmt.Errorf("while decoding message: %s", err)
	}

	if DEBUG_WIRE {
		fmt.Printf(">> %s %+v\n", reflect.TypeOf(msg).Elem().Name(), msg)
	}

	return nil
}

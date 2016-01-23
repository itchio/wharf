package counter

import "io"

type CounterWriter struct {
	count  int64
	writer io.Writer

	onWrite CountCallback
}

func NewWriter(writer io.Writer) *CounterWriter {
	return &CounterWriter{writer: writer}
}

func NewWriterCallback(onWrite CountCallback, writer io.Writer) *CounterWriter {
	return &CounterWriter{
		writer:  writer,
		onWrite: onWrite,
	}
}

func (w *CounterWriter) Count() int64 {
	return w.count
}

func (w *CounterWriter) Write(buffer []byte) (n int, err error) {
	if w.writer == nil {
		n = len(buffer)
	} else {
		n, err = w.writer.Write(buffer)
	}

	w.count += int64(n)
	if w.onWrite != nil {
		w.onWrite(w.count)
	}
	return
}

func (w *CounterWriter) Close() error {
	return nil
}

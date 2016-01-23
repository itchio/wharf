package counter

import "io"

type CounterReader struct {
	count  int64
	reader io.Reader

	onRead CountCallback
}

func NewReader(reader io.Reader) *CounterReader {
	return &CounterReader{reader: reader}
}

func NewReaderCallback(onRead CountCallback, reader io.Reader) *CounterReader {
	return &CounterReader{
		reader: reader,
		onRead: onRead,
	}
}

func (r *CounterReader) Count() int64 {
	return r.count
}

func (r *CounterReader) Read(buffer []byte) (n int, err error) {
	if r.reader == nil {
		n = len(buffer)
	} else {
		n, err = r.reader.Read(buffer)
	}

	r.count += int64(n)
	if r.onRead != nil {
		r.onRead(r.count)
	}
	return
}

func (r *CounterReader) Close() error {
	return nil
}

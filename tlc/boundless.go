package tlc

import "io"

func (r *BoundlessReader) Read(p []byte) (int, error) {
	offset := 0
	buflen := len(p)

	for offset < buflen {
		n, err := r.reader.Read(p[offset:])
		offset += n

		if err != nil {
			if err == ErrFileBoundary {
				continue
			}
			return offset, err
		}
	}

	return offset, nil
}

func (r *BoundlessReader) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

func (reader *Reader) Boundless() io.ReadSeeker {
	return &BoundlessReader{reader}
}

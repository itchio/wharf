package tlc

import (
	"io"
	"os"
	"path/filepath"
)

type Reader struct {
	basePath  string
	container *Container

	offset    int64
	fileIndex int
	reader    io.ReadSeeker
}

type BoundlessReader struct {
	reader *Reader
}

func (c *Container) NewReader(basePath string) *Reader {
	return &Reader{
		basePath:  basePath,
		container: c,

		offset:    0,
		fileIndex: 0,
		reader:    nil,
	}
}

func (r *Reader) Read(p []byte) (int, error) {
	// tlcprint("Read(p [%d]byte)", len(p))

	if len(p) == 0 {
		return 0, nil
	}

	file := r.container.Files[r.fileIndex]

	if r.reader != nil && r.offset >= file.Offset && r.offset < file.OffsetEnd {
		// all good!
	} else {
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	n, err := r.reader.Read(p)
	r.offset += int64(n)

	if err == io.EOF {
		if r.fileIndex+1 >= len(r.container.Files) {
			return n, io.EOF
		} else {
			return n, ErrFileBoundary
		}
	}
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	tlcprint("RSeek(offset %d, whence %d)", offset, whence)

	newOffset, err := seekToNewOffset(r.offset, r.container.Size, offset, whence)
	if err != nil {
		return 0, err
	}

	file := r.container.Files[r.fileIndex]

	if r.reader != nil &&
		newOffset >= file.Offset &&
		newOffset < file.OffsetEnd {

		// tlcprint("RSeek() - has reader %s (%d <= %d < %d)", file.Path, file.BlockIndex, blockIndex, file.BlockIndexEnd)
		inFileOffset := newOffset - file.Offset

		// tlcprint("RSeek() - seeking infile to %d", inFileOffset)
		newInFileOffset, err := r.reader.Seek(inFileOffset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		return newInFileOffset + file.Offset, nil
	}

	// tlcprint("RSeek() - no reader")

	r.fileIndex = r.container.offsetToFileIndex(newOffset)
	if r.fileIndex >= len(r.container.Files) {
		return 0, io.EOF
	}

	// tlcprint("RSeek() - fileIndex = %d", r.fileIndex)

	err = r.Close()
	if err != nil {
		return 0, err
	}

	file = r.container.Files[r.fileIndex]
	tlcprint("RSeek() - opening %s", file.Path)

	fullPath := filepath.Join(r.basePath, file.Path)
	reader, err := os.Open(fullPath)
	if err != nil {
		return 0, err
	}

	inFileOffset := newOffset - file.Offset

	// tlcprint("RSeek() - newOffset = %d, fileOffset = %d", newOffset, fileOffset)
	// tlcprint("RSeek() - seeking to %d", inFileOffset)

	_, err = reader.Seek(inFileOffset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	r.reader = reader
	r.offset = newOffset

	// tlcprint("RSeek() - all done, newOffset = %d", newOffset)

	return newOffset, nil
}

func (r *Reader) Close() error {
	if r.reader != nil {
		if cl, ok := r.reader.(io.Closer); ok {
			err := cl.Close()
			if err != nil {
				return err
			}
		}
		r.reader = nil
	}

	return nil
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

var _ io.ReadSeeker = (*Reader)(nil)
var _ io.Closer = (*Reader)(nil)

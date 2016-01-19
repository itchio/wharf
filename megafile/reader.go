package megafile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Reader struct {
	basePath string
	info     *RepoInfo

	offset    int64
	fileIndex int
	reader    io.ReadSeeker
}

func (info *RepoInfo) NewReader(basePath string) *Reader {
	return &Reader{
		basePath: basePath,
		info:     info,

		offset:    0,
		fileIndex: 0,
		reader:    nil,
	}
}

func (r *Reader) Read(p []byte) (int, error) {
	fmt.Printf("Read(p [%d]byte)\n", len(p))

	blockSize := int64(r.info.BlockSize)

	if r.reader == nil {
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	file := r.info.Files[r.fileIndex]
	fmt.Printf("Read() - in file %s\n", file.Path)

	fileBoundary := file.BlockIndexEnd * blockSize
	fmt.Printf("Read() - file boundary: %d\n", fileBoundary)

	numFiles := len(r.info.Files)
	for r.offset == fileBoundary && r.fileIndex < numFiles {
		fmt.Printf("Read() - at file boundary, seeking\n")
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	// bytesLeftInFile := fileBoundary - r.offset
	// copied := min(int64(len(p)), bytesLeftInFile)

	n, err := r.reader.Read(p)
	fmt.Printf("read %d / %s\n", n, err)

	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	fmt.Printf("Seek(offset %d, whence %d)\n", offset, whence)

	blockSize := int64(r.info.BlockSize)
	totalSize := blockSize * int64(r.info.NumBlocks)

	var newOffset int64

	switch whence {
	case os.SEEK_SET:
		newOffset = offset
	case os.SEEK_CUR:
		newOffset = r.offset + offset
	case os.SEEK_END:
		newOffset = totalSize - offset
	}

	fmt.Printf("Seek() - newOffset = %d\n", newOffset)

	if newOffset < 0 {
		newOffset = 0
	}

	blockIndex := newOffset / blockSize
	file := r.info.Files[r.fileIndex]

	fmt.Printf("Seek() - blockIndex = %d\n", blockIndex)

	if r.reader != nil &&
		file.BlockIndex <= blockIndex &&
		blockIndex < file.BlockIndexEnd {

		fmt.Printf("Seek() - has reader\n")

		fileOffset := file.BlockIndex * blockSize
		inFileOffset := offset - fileOffset

		_, err := r.reader.Seek(inFileOffset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		return newOffset, nil
	}

	fmt.Printf("Seek() - no reader\n")

	// binary search to find the file that contains our block
	lb := 0
	rb := len(r.info.Files)

	fmt.Printf("Seek() - lb = %d, rb = %d\n", lb, rb)

	for rb-lb > 1 {
		mb := (lb + rb) / 2
		fmt.Printf("Seek() - lb = %d, mb = %d, rb = %d\n", lb, mb, rb)

		file = r.info.Files[mb]
		if file.BlockIndex < blockIndex {
			// pick the left half of our search interval (move the right boundary)
			rb = mb
		} else if file.BlockIndexEnd > blockIndex {
			// pick the right half of our search interval (move the left boundary)
			lb = mb
		} else {
			// found it!
			r.fileIndex = mb
			break
		}
	}

	// skip over empty files
	for r.info.Files[r.fileIndex].BlockIndexEnd == r.info.Files[r.fileIndex].BlockIndex {
		r.fileIndex++
	}

	fmt.Printf("Seek() - fileIndex = %d\n", r.fileIndex)

	err := r.Close()
	if err != nil {
		return 0, err
	}

	file = r.info.Files[r.fileIndex]
	fmt.Printf("Seek() - opening %s\n", file.Path)

	fullPath := filepath.Join(r.basePath, file.Path)
	reader, err := os.Open(fullPath)
	if err != nil {
		return 0, err
	}

	fileOffset := file.BlockIndex * blockSize
	inFileOffset := newOffset - fileOffset

	fmt.Printf("Seek() - newOffset = %d, fileOffset = %d\n", newOffset, fileOffset)
	fmt.Printf("Seek() - seeking to %d\n", inFileOffset)

	_, err = reader.Seek(inFileOffset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	r.reader = reader
	r.offset = newOffset

	fmt.Printf("Seek() - all done, newOffset = %d\n", newOffset)

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

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
	fileAlignedBoundary := file.BlockIndexEnd * blockSize
	fmt.Printf("Read() - in file %s\n", file.Path)

	numFiles := len(r.info.Files)
	if r.offset == fileAlignedBoundary && r.fileIndex < numFiles {
		fmt.Printf("Read() - offset %d = file boundary %d, seeking\n", r.offset, fileAlignedBoundary)
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		file = r.info.Files[r.fileIndex]
		fileAlignedBoundary = file.BlockIndexEnd * blockSize
	}

	fileRealBoundary := file.BlockIndex*blockSize + file.Size
	fmt.Printf("Read() - file boundaries: real %d, aligned %d\n", fileRealBoundary, fileAlignedBoundary)

	n, err := r.reader.Read(p)
	fmt.Printf("Read() - got %d bytes, err = %s, new offset = %d\n", n, err, r.offset)

	if err == io.EOF {
		paddingBytesLeft := fileAlignedBoundary - r.offset
		copied := min(int64(len(p)), paddingBytesLeft)
		fmt.Printf("Read() - %d padding bytes left, copying %d\n", paddingBytesLeft, copied)

		// pad with 0s
		for i := int64(0); i < copied; i++ {
			p[i] = 0
		}
		r.offset += copied
		return int(copied), nil
	}

	r.offset += int64(n)
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	fmt.Printf("Seek(offset %d, whence %d)\n", offset, whence)

	blockSize := int64(r.info.BlockSize)
	totalSize := blockSize * int64(r.info.NumBlocks)

	newOffset, err := seekToNewOffset(r.offset, totalSize, offset, whence)
	fmt.Printf("Seek() - newOffset = %d\n", newOffset)
	if err != nil {
		return 0, err
	}

	blockIndex := newOffset / blockSize
	file := r.info.Files[r.fileIndex]

	fmt.Printf("Seek() - blockIndex = %d\n", blockIndex)

	if r.reader != nil &&
		blockIndex >= file.BlockIndex &&
		blockIndex < file.BlockIndexEnd {

		fmt.Printf("Seek() - has reader %s (%d <= %d < %d)\n", file.Path, file.BlockIndex, blockIndex, file.BlockIndexEnd)

		fileOffset := file.BlockIndex * blockSize
		inFileOffset := offset - fileOffset

		fmt.Printf("Seek() - seeking infile to %d\n", inFileOffset)
		_, err := r.reader.Seek(inFileOffset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		return newOffset, nil
	}

	fmt.Printf("Seek() - no reader\n")

	r.fileIndex = r.info.blockIndexToFileIndex(blockIndex)
	fmt.Printf("Seek() - fileIndex = %d\n", r.fileIndex)

	err = r.Close()
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

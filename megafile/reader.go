package megafile

import (
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
	megaprint("Read(p [%d]byte)\n", len(p))

	blockSize := int64(r.info.BlockSize)

	if r.reader == nil {
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	file := r.info.Files[r.fileIndex]
	fileAlignedBoundary := file.BlockIndexEnd * blockSize
	megaprint("Read() - in file %s\n", file.Path)

	numFiles := len(r.info.Files)
	if r.offset == fileAlignedBoundary {
		if r.fileIndex >= numFiles-1 {
			return 0, io.EOF
		}

		megaprint("Read() - offset %d = file boundary %d, file Index = %d / %d seeking\n", r.offset, fileAlignedBoundary, r.fileIndex, numFiles)
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		file = r.info.Files[r.fileIndex]
		fileAlignedBoundary = file.BlockIndexEnd * blockSize
	}

	fileRealBoundary := file.BlockIndex*blockSize + file.Size
	megaprint("Read() - file boundaries: real %d, aligned %d\n", fileRealBoundary, fileAlignedBoundary)

	n, err := r.reader.Read(p)
	megaprint("Read() - got %d bytes, err = %s, new offset = %d\n", n, err, r.offset)

	if err == io.EOF {
		paddingBytesLeft := fileAlignedBoundary - r.offset
		copied := min(int64(len(p)), paddingBytesLeft)
		megaprint("Read() - %d padding bytes left, copying %d\n", paddingBytesLeft, copied)

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
	megaprint("Seek(offset %d, whence %d)\n", offset, whence)

	blockSize := int64(r.info.BlockSize)
	totalSize := blockSize * int64(r.info.NumBlocks)

	newOffset, err := seekToNewOffset(r.offset, totalSize, offset, whence)
	megaprint("Seek() - newOffset = %d\n", newOffset)
	if err != nil {
		return 0, err
	}

	blockIndex := newOffset / blockSize
	file := r.info.Files[r.fileIndex]

	megaprint("Seek() - blockIndex = %d\n", blockIndex)

	if r.reader != nil &&
		blockIndex >= file.BlockIndex &&
		blockIndex < file.BlockIndexEnd {

		megaprint("Seek() - has reader %s (%d <= %d < %d)\n", file.Path, file.BlockIndex, blockIndex, file.BlockIndexEnd)

		fileOffset := file.BlockIndex * blockSize
		inFileOffset := offset - fileOffset

		megaprint("Seek() - seeking infile to %d\n", inFileOffset)
		_, err := r.reader.Seek(inFileOffset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		return newOffset, nil
	}

	megaprint("Seek() - no reader\n")

	r.fileIndex = r.info.blockIndexToFileIndex(blockIndex)
	megaprint("Seek() - fileIndex = %d\n", r.fileIndex)

	err = r.Close()
	if err != nil {
		return 0, err
	}

	file = r.info.Files[r.fileIndex]
	megaprint("Seek() - opening %s\n", file.Path)

	fullPath := filepath.Join(r.basePath, file.Path)
	reader, err := os.Open(fullPath)
	if err != nil {
		return 0, err
	}

	fileOffset := file.BlockIndex * blockSize
	inFileOffset := newOffset - fileOffset

	megaprint("Seek() - newOffset = %d, fileOffset = %d\n", newOffset, fileOffset)
	megaprint("Seek() - seeking to %d\n", inFileOffset)

	_, err = reader.Seek(inFileOffset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	r.reader = reader
	r.offset = newOffset

	megaprint("Seek() - all done, newOffset = %d\n", newOffset)

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

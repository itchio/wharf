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
	// megaprint("Read(p [%d]byte)", len(p))

	if len(p) == 0 {
		return 0, nil
	}

	blockSize := int64(r.info.BlockSize)

	if r.reader == nil {
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	file := r.info.Files[r.fileIndex]
	fileAlignedBoundary := file.BlockIndexEnd * blockSize
	// megaprint("Read() - in file %s", file.Path)

	numFiles := len(r.info.Files)
	if r.offset == fileAlignedBoundary {
		if r.fileIndex >= numFiles-1 {
			megaprint("Read() - EOF!")
			return 0, io.EOF
		}

		// megaprint("Read() - offset %d = file boundary %d, file Index = %d / %d seeking", r.offset, fileAlignedBoundary, r.fileIndex, numFiles)
		_, err := r.Seek(r.offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		file = r.info.Files[r.fileIndex]
		fileAlignedBoundary = file.BlockIndexEnd * blockSize
	}

	// fileRealBoundary := file.BlockIndex*blockSize + file.Size
	// megaprint("Read() - file boundaries: real %d, aligned %d", fileRealBoundary, fileAlignedBoundary)

	n, err := r.reader.Read(p)
	if n > 0 {
		r.offset += int64(n)
		megaprint("Read() - read %d bytes from %s, new offset = %d", n, file.Path, r.offset)

		if err == io.EOF {
			err = nil
		}
		return n, err
	} else {
		if err == io.EOF {
			paddingBytesLeft := fileAlignedBoundary - r.offset
			copied := min(int64(len(p)), paddingBytesLeft)
			if copied > 0 {
				// pad with 0s
				for i := int64(0); i < copied; i++ {
					p[i] = 0
				}
				r.offset += copied
				megaprint("Read() - read %d padding bytes from %s, new offset = %d", copied, file.Path, r.offset)
				return int(copied), nil
			} else {
				megaprint("Read() - out of padding bytes, returning eof")
				return 0, io.EOF
			}
		} else {
			megaprint("Read() - some other error happened: %s", err.Error())
			return 0, err
		}
	}
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	megaprint("RSeek(offset %d, whence %d)", offset, whence)

	blockSize := int64(r.info.BlockSize)
	totalSize := blockSize * int64(r.info.NumBlocks)

	newOffset, err := seekToNewOffset(r.offset, totalSize, offset, whence)
	// megaprint("RSeek() - newOffset = %d", newOffset)
	if err != nil {
		return 0, err
	}

	blockIndex := newOffset / blockSize
	file := r.info.Files[r.fileIndex]

	// megaprint("RSeek() - blockIndex = %d", blockIndex)

	if r.reader != nil &&
		blockIndex >= file.BlockIndex &&
		blockIndex < file.BlockIndexEnd {

		// megaprint("RSeek() - has reader %s (%d <= %d < %d)", file.Path, file.BlockIndex, blockIndex, file.BlockIndexEnd)

		fileOffset := file.BlockIndex * blockSize
		inFileOffset := offset - fileOffset

		// megaprint("RSeek() - seeking infile to %d", inFileOffset)
		_, err := r.reader.Seek(inFileOffset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		return newOffset, nil
	}

	// megaprint("RSeek() - no reader")

	r.fileIndex = r.info.blockIndexToFileIndex(blockIndex)
	// megaprint("RSeek() - fileIndex = %d", r.fileIndex)

	err = r.Close()
	if err != nil {
		return 0, err
	}

	file = r.info.Files[r.fileIndex]
	megaprint("RSeek() - opening %s", file.Path)

	fullPath := filepath.Join(r.basePath, file.Path)
	reader, err := os.Open(fullPath)
	if err != nil {
		return 0, err
	}

	fileOffset := file.BlockIndex * blockSize
	inFileOffset := newOffset - fileOffset

	// megaprint("RSeek() - newOffset = %d, fileOffset = %d", newOffset, fileOffset)
	// megaprint("RSeek() - seeking to %d", inFileOffset)

	_, err = reader.Seek(inFileOffset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	r.reader = reader
	r.offset = newOffset

	// megaprint("RSeek() - all done, newOffset = %d", newOffset)

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

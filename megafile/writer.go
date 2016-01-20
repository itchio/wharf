package megafile

import (
	"io"
	"os"
	"path/filepath"
)

type Writer struct {
	basePath string
	info     *RepoInfo

	offset    int64
	fileIndex int
	writer    io.WriteSeeker
}

func (info *RepoInfo) NewWriter(basePath string) (*Writer, error) {
	for _, dirEntry := range info.Dirs {
		fullPath := filepath.Join(basePath, dirEntry.Path)
		megaprint("mkdir -p %s %d\n", fullPath, dirEntry.Mode)
		err := os.MkdirAll(fullPath, dirEntry.Mode)
		if err != nil {
			return nil, err
		}
		err = os.Chmod(fullPath, dirEntry.Mode)
		if err != nil {
			return nil, err
		}
	}

	for _, fileEntry := range info.Files {
		fullPath := filepath.Join(basePath, fileEntry.Path)
		megaprint("touch %s %d\n", fullPath, fileEntry.Mode)
		file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC, fileEntry.Mode)
		if err != nil {
			return nil, err
		}
		err = file.Close()
		if err != nil {
			return nil, err
		}

		err = os.Chmod(fullPath, fileEntry.Mode)
		if err != nil {
			return nil, err
		}

		err = os.Truncate(fullPath, fileEntry.Size)
		if err != nil {
			return nil, err
		}
	}

	for _, link := range info.Symlinks {
		fullPath := filepath.Join(basePath, link.Path)
		megaprint("ln -s %s %s\n", link.Dest, fullPath)
		err := os.Symlink(link.Dest, fullPath)
		if err != nil {
			return nil, err
		}
	}

	return &Writer{
		basePath: basePath,
		info:     info,

		offset:    0,
		fileIndex: 0,
		writer:    nil,
	}, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	megaprint("Write(p [%d]byte)\n", len(p))

	blockSize := int64(w.info.BlockSize)

	bufferOffset := 0
	bufferSize := len(p)

	for bufferOffset < bufferSize {
		bufferLeft := bufferSize - bufferOffset

		if w.writer == nil {
			_, err := w.Seek(w.offset, os.SEEK_SET)
			if err != nil {
				return 0, err
			}
		}

		file := w.info.Files[w.fileIndex]
		fileAlignedBoundary := file.BlockIndexEnd * blockSize
		megaprint("Write() - in file %s\n", file.Path)

		fileRealBoundary := file.BlockIndex*blockSize + file.Size
		megaprint("Write() - file boundaries: real %d, aligned %d\n", fileRealBoundary, fileAlignedBoundary)

		written := 0

		if w.offset < fileRealBoundary {
			realBytesLeft := min(int64(bufferLeft), fileRealBoundary-w.offset)
			megaprint("Write() - offset %d, writing %d real bytes\n", w.offset, realBytesLeft)
			copied, err := w.writer.Write(p[bufferOffset : bufferOffset+int(realBytesLeft)])
			if err != nil {
				return 0, err
			}
			written = copied
		} else if w.offset < fileAlignedBoundary {
			paddingBytesLeft := min(int64(bufferLeft), fileAlignedBoundary-w.offset)
			megaprint("Write() - offset %d, ignoring %d padding bytes\n", w.offset, paddingBytesLeft)
			written = int(paddingBytesLeft)
		} else {
			megaprint("Write() - offset %d, file boundary %d, seeking\n", w.offset, fileAlignedBoundary)
			_, err := w.Seek(w.offset, os.SEEK_SET)
			if err != nil {
				return 0, err
			}

			file = w.info.Files[w.fileIndex]
			fileAlignedBoundary = file.BlockIndexEnd * blockSize
		}

		bufferOffset += written
		w.offset += int64(written)

		megaprint("Write() - wrote %d bytes, new offset = %d\n", written, w.offset)
	}

	megaprint("Write() - wrote everything! success!")
	return bufferSize, nil
}

func (w *Writer) Seek(offset int64, whence int) (int64, error) {
	megaprint("Seek(offset %d, whence %d)\n", offset, whence)

	blockSize := int64(w.info.BlockSize)
	totalSize := blockSize * int64(w.info.NumBlocks)

	newOffset, err := seekToNewOffset(w.offset, totalSize, offset, whence)
	megaprint("Seek() - newOffset = %d\n", newOffset)
	if err != nil {
		return 0, err
	}

	blockIndex := newOffset / blockSize
	file := w.info.Files[w.fileIndex]

	megaprint("Seek() - blockIndex = %d\n", blockIndex)

	if w.writer != nil &&
		blockIndex >= file.BlockIndex &&
		blockIndex < file.BlockIndexEnd {

		megaprint("Seek() - has writer %s (%d <= %d < %d)\n", file.Path, file.BlockIndex, blockIndex, file.BlockIndexEnd)

		fileOffset := file.BlockIndex * blockSize
		inFileOffset := offset - fileOffset

		megaprint("Seek() - seeking infile to %d\n", inFileOffset)
		_, err := w.writer.Seek(inFileOffset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		return newOffset, nil
	}

	megaprint("Seek() - no writer\n")

	w.fileIndex = w.info.blockIndexToFileIndex(blockIndex)
	megaprint("Seek() - fileIndex = %d\n", w.fileIndex)

	err = w.Close()
	if err != nil {
		return 0, err
	}

	file = w.info.Files[w.fileIndex]
	megaprint("Seek() - opening %s\n", file.Path)

	fullPath := filepath.Join(w.basePath, file.Path)
	writer, err := os.OpenFile(fullPath, os.O_WRONLY, 0)
	if err != nil {
		return 0, err
	}

	fileOffset := file.BlockIndex * blockSize
	inFileOffset := newOffset - fileOffset

	megaprint("Seek() - newOffset = %d, fileOffset = %d\n", newOffset, fileOffset)
	megaprint("Seek() - seeking to %d\n", inFileOffset)

	_, err = writer.Seek(inFileOffset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	w.writer = writer
	w.offset = newOffset

	megaprint("Seek() - all done, newOffset = %d\n", newOffset)

	return newOffset, nil
}

func (w *Writer) Close() error {
	if w.writer != nil {
		if cl, ok := w.writer.(io.Closer); ok {
			err := cl.Close()
			if err != nil {
				return err
			}
		}
		w.writer = nil
	}

	return nil
}

var _ io.WriteSeeker = (*Writer)(nil)
var _ io.Closer = (*Writer)(nil)

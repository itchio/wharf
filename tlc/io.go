package tlc

import (
	"errors"
	"fmt"
	"os"
)

var (
	ErrInvalid = errors.New("invalid arguments supplied")
)

var tlc_DEBUG = false

func megaprint(format string, args ...interface{}) {
	if tlc_DEBUG {
		fmt.Printf(format, args...)
		fmt.Print("\n")
	}
}

func (info *RepoInfo) blockIndexToFileIndex(blockIndex int64) int {
	fileIndex := 0
	file := info.Files[fileIndex]

	// binary search to find the file that contains our block
	lb := 0
	rb := len(info.Files)

	// megaprint("offsetToFileIndex() - lb = %d, rb = %d", lb, rb)

	for {
		mb := (lb + rb) / 2
		if mb == lb || mb == rb {
			// found it!
			fileIndex = mb
			// megaprint("offsetToFileIndex() - found at %d", fileIndex)
			break
		}

		file = info.Files[mb]
		if blockIndex < file.BlockIndex {
			// megaprint("offsetToFileIndex() - blockIndex %d < file.BlockIndex %d, picking left", blockIndex, file.BlockIndex)
			// pick the left half of our search interval (move the right boundary)
			rb = mb
		} else if blockIndex >= file.BlockIndexEnd {
			// pick the right half of our search interval (move the left boundary)
			// megaprint("offsetToFileIndex() - blockIndex %d > file.BlockIndexEnd %d, picking right", blockIndex, file.BlockIndexEnd)
			lb = mb
		} else {
			// found it!
			fileIndex = mb
			// megaprint("offsetToFileIndex() - found at %d", fileIndex)
			break
		}
	}

	// skip over empty files
	for info.Files[fileIndex].BlockIndexEnd == info.Files[fileIndex].BlockIndex {
		// megaprint("offsetToFileIndex() - skipping over empty file at %d", fileIndex)
		fileIndex++
	}

	// megaprint("offsetToFileIndex() - fileIndex = %d", fileIndex)

	return fileIndex
}

func seekToNewOffset(oldOffset int64, totalSize int64, offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case os.SEEK_SET:
		newOffset = offset
	case os.SEEK_CUR:
		newOffset = oldOffset + offset
	case os.SEEK_END:
		newOffset = totalSize - offset
	}

	if newOffset < 0 || newOffset >= totalSize {
		return 0, ErrInvalid
	}

	return newOffset, nil
}

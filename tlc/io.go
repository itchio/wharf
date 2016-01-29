package tlc

import (
	"errors"
	"fmt"
	"os"
)

var (
	ErrInvalid      = errors.New("invalid arguments supplied")
	ErrFileBoundary = errors.New("reached file boundary")
)

var TLC_DEBUG = false

func tlcprint(format string, args ...interface{}) {
	if TLC_DEBUG {
		fmt.Printf(format, args...)
		fmt.Print("\n")
	}
}

func (c *Container) offsetToFileIndex(offset int64) int {
	fileIndex := 0
	file := c.Files[fileIndex]

	// binary search to find the file that contains our block
	lb := 0
	rb := len(c.Files)

	// tlcprint("offsetToFileIndex() - lb = %d, rb = %d", lb, rb)

	for {
		mb := (lb + rb) / 2
		if mb == lb || mb == rb {
			// found it!
			fileIndex = mb
			// tlcprint("offsetToFileIndex() - found at %d", fileIndex)
			break
		}

		file = c.Files[mb]
		if offset < file.Offset {
			// tlcprint("offsetToFileIndex() - blockIndex %d < file.BlockIndex %d, picking left", blockIndex, file.BlockIndex)
			// pick the left half of our search interval (move the right boundary)
			rb = mb
		} else if offset >= file.OffsetEnd {
			// pick the right half of our search interval (move the left boundary)
			// tlcprint("offsetToFileIndex() - blockIndex %d > file.BlockIndexEnd %d, picking right", blockIndex, file.BlockIndexEnd)
			lb = mb
		} else {
			// found it!
			fileIndex = mb
			// tlcprint("offsetToFileIndex() - found at %d", fileIndex)
			break
		}
	}

	// skip over empty files
	for c.Files[fileIndex].OffsetEnd == c.Files[fileIndex].Offset {
		// tlcprint("offsetToFileIndex() - skipping over empty file at %d", fileIndex)
		fileIndex++
	}

	// tlcprint("offsetToFileIndex() - fileIndex = %d", fileIndex)

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

	if newOffset < 0 {
		return 0, ErrInvalid
	}

	return newOffset, nil
}

package bowl

import (
	"io"
)

type Bowl interface {
	// phase 1: patching
	GetWriter(index int64) (io.WriteCloser, error)
	Transpose(transposition Transposition) error

	// phase 2: committing
	Commit() error
}

type Transposition struct {
	TargetIndex int64
	SourceIndex int64
}

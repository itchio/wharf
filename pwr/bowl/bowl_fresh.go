package bowl

import (
	"io"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wsync"
)

type freshBowl struct {
	TargetContainer *tlc.Container
	SourceContainer *tlc.Container
	TargetPath      string

	TargetPool wsync.Pool
	OutputPool wsync.WritablePool

	buf []byte
}

const freshBufferSize = 32 * 1024

var _ Bowl = (*freshBowl)(nil)

type FreshBowlParams struct {
	TargetContainer *tlc.Container
	SourceContainer *tlc.Container

	TargetPool   wsync.Pool
	OutputFolder string
	OutputPool   wsync.WritablePool
}

// NewFreshBowl returns a bowl that applies all writes to
// a given (initially empty) directory, or a custom OutputPool
func NewFreshBowl(params *FreshBowlParams) (Bowl, error) {
	// input validation

	if params.TargetContainer == nil {
		return nil, errors.New("freshbowl: TargetContainer must not be nil")
	}

	if params.TargetPool == nil {
		return nil, errors.New("freshbowl: TargetPool must not be nil")
	}

	if params.SourceContainer == nil {
		return nil, errors.New("freshbowl: SourceContainer must not be nil")
	}

	if params.OutputPool == nil {
		if params.OutputFolder == "" {
			// both zero
			return nil, errors.New("freshbowl: must specify either OutputPool or OutputFolder")
		}
	} else {
		if params.OutputFolder != "" {
			// both non-zero
			return nil, errors.New("freshbowl: cannot specify both OutputPool and OutputFolder")
		}
	}

	outputPool := params.OutputPool
	if outputPool == nil {
		outputPool = fspool.New(params.SourceContainer, params.OutputFolder)

		err := params.SourceContainer.Prepare(params.OutputFolder)
		if err != nil {
			return nil, errors.Wrap(err, 0)
		}
	}

	return &freshBowl{
		TargetContainer: params.TargetContainer,
		SourceContainer: params.SourceContainer,
		TargetPool:      params.TargetPool,

		OutputPool: outputPool,
	}, nil
}

func (fb *freshBowl) GetWriter(index int64) (io.WriteCloser, error) {
	return fb.OutputPool.GetWriter(index)
}

func (fb *freshBowl) Transpose(t Transposition) (rErr error) {
	// alright y'all it's copy time

	r, err := fb.TargetPool.GetReader(t.TargetIndex)
	if err != nil {
		rErr = errors.Wrap(err, 0)
		return
	}

	w, err := fb.OutputPool.GetWriter(t.SourceIndex)
	if err != nil {
		rErr = errors.Wrap(err, 0)
		return
	}
	defer func() {
		cErr := w.Close()
		if cErr != nil && rErr == nil {
			rErr = errors.Wrap(cErr, 0)
		}
	}()

	if len(fb.buf) < freshBufferSize {
		fb.buf = make([]byte, freshBufferSize)
	}

	_, err = io.CopyBuffer(w, r, fb.buf)
	if err != nil {
		rErr = errors.Wrap(err, 0)
		return
	}

	return
}

func (fb *freshBowl) Commit() error {
	// it's all done buddy!
	return nil
}

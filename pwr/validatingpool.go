package pwr

import (
	"io"

	"github.com/itchio/lake"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/wharf/pwr/drip"
	"github.com/itchio/wharf/pwr/onclose"
	"github.com/itchio/wharf/wsync"
	"github.com/pkg/errors"
)

type OnCloseFunc func(fileIndex int64)

type WoundsFilterFunc func(wounds chan *Wound) chan *Wound

// A ValidatingPool will check files against their hashes, but doesn't
// check directories or symlinks
type ValidatingPool struct {
	// required

	Pool lake.WritablePool
	// Container must match Pool - may have different file indices than Signature.Container
	Container *tlc.Container
	Signature *SignatureInfo

	Wounds       chan *Wound
	WoundsFilter WoundsFilterFunc

	OnClose OnCloseFunc

	// private //

	hashInfo *HashInfo
	sctx     *wsync.Context
}

var _ lake.WritablePool = (*ValidatingPool)(nil)

// GetSize is a pass-through to the underlying Pool
func (vp *ValidatingPool) GetSize(fileIndex int64) int64 {
	return vp.Pool.GetSize(fileIndex)
}

// GetReader is a pass-through to the underlying Pool, it doesn't validate
func (vp *ValidatingPool) GetReader(fileIndex int64) (io.Reader, error) {
	return vp.GetReadSeeker(fileIndex)
}

// GetReadSeeker is a pass-through to the underlying Pool, it doesn't validate
func (vp *ValidatingPool) GetReadSeeker(fileIndex int64) (io.ReadSeeker, error) {
	return vp.Pool.GetReadSeeker(fileIndex)
}

// GetWriter returns a writer that checks hashes before writing to the underlying
// pool's writer. It tries really hard to be transparent, but does buffer some data,
// which means some writing is only done when the returned writer is closed.
func (vp *ValidatingPool) GetWriter(fileIndex int64) (io.WriteCloser, error) {
	var wounds chan *Wound
	var woundsDone chan bool

	if vp.Wounds != nil {
		wounds = make(chan *Wound)
		originalWounds := wounds
		if vp.WoundsFilter != nil {
			wounds = vp.WoundsFilter(wounds)
		}

		woundsDone = make(chan bool)

		go func() {
			for wound := range originalWounds {
				if vp.Wounds != nil {
					vp.Wounds <- wound
				}
			}
			woundsDone <- true
		}()
	}

	if vp.hashInfo == nil {
		var err error
		vp.hashInfo, err = ComputeHashInfo(vp.Signature)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		vp.sctx = wsync.NewContext(int(BlockSize))
	}

	w, err := vp.Pool.GetWriter(fileIndex)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	bv := NewBlockValidator(vp.hashInfo)
	var blockIndex int64
	validate := func(data []byte) error {
		var err error

		if wounds == nil {
			err = bv.ValidateAsError(fileIndex, blockIndex, data)
		} else {
			wound := bv.ValidateAsWound(fileIndex, blockIndex, data)
			wounds <- &wound
		}

		blockIndex++
		return err
	}

	ocw := &onclose.Writer{
		Writer: w,
		BeforeClose: func() {
			if wounds != nil {
				close(wounds)
				<-woundsDone
			}
		},
		AfterClose: func() {
			if vp.OnClose != nil {
				vp.OnClose(fileIndex)
			}
		},
	}

	dw := &drip.Writer{
		Writer:   ocw,
		Buffer:   make([]byte, BlockSize),
		Validate: validate,
	}

	return dw, nil
}

// Close closes the underlying pool (and its reader, if any)
func (vp *ValidatingPool) Close() error {
	return vp.Pool.Close()
}

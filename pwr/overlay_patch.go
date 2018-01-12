package pwr

import (
	"encoding/gob"
	"io"

	"github.com/go-errors/errors"
)

type OverlayPatchContext struct {
	buf []byte
}

const overlayPatchBufSize = 32 * 1024

func (ctx *OverlayPatchContext) Patch(r io.Reader, w io.WriteSeeker) error {
	decoder := gob.NewDecoder(r)
	op := &OverlayOp{}

	for {
		// reset op
		op.Skip = 0
		op.Fresh = 0
		op.Eof = false

		err := decoder.Decode(op)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		switch {
		case op.Eof:
			// cool, we're done!
			return nil

		case op.Skip > 0:
			_, err = w.Seek(op.Skip, io.SeekCurrent)
			if err != nil {
				return errors.Wrap(err, 0)
			}

		case op.Fresh > 0:
			if len(ctx.buf) < overlayPatchBufSize {
				ctx.buf = make([]byte, overlayPatchBufSize)
			}

			_, err = io.CopyBuffer(w, io.LimitReader(r, op.Fresh), ctx.buf)
			if err != nil {
				return errors.Wrap(err, 0)
			}
		}
	}
}

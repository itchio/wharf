package pwr

import (
	"io"
	"os"

	humanize "github.com/dustin/go-humanize"
	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pools"
	"github.com/itchio/wharf/pools/nullpool"
)

type ValidatorContext struct {
	WoundsPath string

	Consumer *StateConsumer
}

func (vc *ValidatorContext) Validate(target string, signature *SignatureInfo) error {
	var woundsWriter *WoundsWriter
	wounds := make(chan *Wound)
	errs := make(chan error)
	done := make(chan bool)

	if vc.WoundsPath == "" {
		go func() {
			for wound := range wounds {
				file := signature.Container.Files[wound.FileIndex]
				woundSize := humanize.IBytes(uint64(wound.BlockSpan * int64(BlockSize)))
				offset := humanize.IBytes(uint64(wound.BlockIndex * int64(BlockSize)))
				vc.Consumer.Infof("~%s wound %s into %s", woundSize, offset, file.Path)
			}
			done <- true
		}()
	} else {
		woundsWriter = &WoundsWriter{
			Wounds: wounds,
		}

		go func() {
			err := woundsWriter.Go(signature, vc.WoundsPath)
			if err != nil {
				errs <- err
				return
			}
			done <- true
		}()
	}

	targetPool, err := pools.New(signature.Container, target)
	if err != nil {
		return err
	}

	validatingPool := &ValidatingPool{
		Pool:      nullpool.New(signature.Container),
		Container: signature.Container,
		Signature: signature,

		Wounds: wounds,
	}

	for i, f := range signature.Container.Files {
		fileIndex := int64(i)

		reader, err := targetPool.GetReader(fileIndex)
		if err != nil {
			if os.IsNotExist(err) {
				// whole file is missing!
				wounds <- &Wound{
					FileIndex:  fileIndex,
					BlockIndex: 0,
					BlockSpan:  numBlocks(f.Size),
				}
			} else {
				return err
			}
		}

		writer, err := validatingPool.GetWriter(fileIndex)
		if err != nil {
			return errors.Wrap(err, 1)
		}

		_, err = io.Copy(writer, reader)
		if err != nil {
			// TODO: handle ErrUnexpectedEOF (short files)
			return err
		}
	}

	select {
	case err := <-errs:
		return err
	case <-done:
		// good!
	}

	return nil
}

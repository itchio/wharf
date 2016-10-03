package pwr

import (
	"io"
	"os"
	"runtime"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pools"
	"github.com/itchio/wharf/pools/nullpool"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wsync"
)

type ValidatorContext struct {
	WoundsPath string
	NumWorkers int

	Consumer *StateConsumer

	// internal
	Container      *tlc.Container
	TargetPool     wsync.Pool
	ValidatingPool wsync.WritablePool
	Wounds         chan *Wound
}

func (vctx *ValidatorContext) Validate(target string, signature *SignatureInfo) error {
	var woundsWriter *WoundsWriter
	vctx.Wounds = make(chan *Wound)
	errs := make(chan error)
	done := make(chan bool)

	if vctx.WoundsPath == "" {
		woundsPrinter := &WoundsPrinter{
			Wounds: vctx.Wounds,
		}

		go func() {
			err := woundsPrinter.Do(signature, vctx.Consumer)
			if err != nil {
				errs <- err
				return
			}
			done <- true
		}()
	} else {
		woundsWriter = &WoundsWriter{
			Wounds: vctx.Wounds,
		}

		go func() {
			err := woundsWriter.Do(signature, vctx.WoundsPath)
			if err != nil {
				errs <- err
				return
			}
			done <- true
		}()
	}

	var err error
	vctx.TargetPool, err = pools.New(signature.Container, target)
	if err != nil {
		return err
	}

	vctx.Container = signature.Container

	vctx.ValidatingPool = &ValidatingPool{
		Pool:      nullpool.New(signature.Container),
		Container: signature.Container,
		Signature: signature,

		Wounds: vctx.Wounds,
	}

	numWorkers := vctx.NumWorkers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU() + 1
	}

	fileIndices := make(chan int64)

	for i := 0; i < numWorkers; i++ {
		go vctx.validate(fileIndices, done, errs)
	}

	for fileIndex := range signature.Container.Files {
		fileIndices <- int64(fileIndex)
	}

	err = vctx.TargetPool.Close()
	if err != nil {
		return errors.Wrap(err, 1)
	}

	close(vctx.Wounds)

	for i := 0; i < numWorkers+1; i++ {
		select {
		case err := <-errs:
			return err
		case <-done:
			// good!
		}
	}

	return nil
}

func (vctx *ValidatorContext) validate(fileIndices chan int64, done chan bool, errs chan error) {
	for fileIndex := range fileIndices {
		file := vctx.Container.Files[fileIndex]

		reader, err := vctx.TargetPool.GetReader(fileIndex)
		if err != nil {
			if os.IsNotExist(err) {
				// that's one big wound
				vctx.Wounds <- &Wound{
					FileIndex: fileIndex,
					Start:     0,
					End:       file.Size,
				}
				continue
			} else {
				errs <- err
				return
			}
		}

		var writer io.WriteCloser
		writer, err = vctx.ValidatingPool.GetWriter(fileIndex)
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		writtenBytes, err := io.Copy(writer, reader)
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		if writtenBytes != file.Size {
			vctx.Wounds <- &Wound{
				FileIndex: fileIndex,
				Start:     writtenBytes,
				End:       file.Size,
			}
			vctx.Consumer.Infof("short file: expected %d, got %d", writtenBytes, file.Size)
		}

		err = writer.Close()
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}
	}
}

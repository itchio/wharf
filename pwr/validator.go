package pwr

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/pools"
	"github.com/itchio/wharf/pools/nullpool"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/wsync"
)

const MaxWoundSize int64 = 4 * 1024 * 1024 // 4MB

type ValidatorContext struct {
	WoundsPath string
	NumWorkers int

	Consumer *state.Consumer

	// FailFast makes Validate return Wounds as errors and stop checking
	FailFast bool

	// Result
	TotalCorrupted int64

	// internal
	TargetPool wsync.Pool
	Wounds     chan *Wound
}

func (vctx *ValidatorContext) Validate(target string, signature *SignatureInfo) error {
	var woundsConsumer WoundsConsumer

	numWorkers := vctx.NumWorkers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU() + 1
	}

	vctx.Wounds = make(chan *Wound)
	errs := make(chan error, numWorkers+1)
	done := make(chan bool, numWorkers+1)
	cancelled := make(chan struct{})

	if vctx.FailFast {
		if vctx.WoundsPath != "" {
			return fmt.Errorf("Validate: FailFast is not compatible with WoundsPath")
		}

		woundsConsumer = &WoundsGuardian{}
	} else if vctx.WoundsPath == "" {
		woundsConsumer = &WoundsPrinter{
			Consumer: vctx.Consumer,
		}
	} else {
		woundsConsumer = &WoundsWriter{
			WoundsPath: vctx.WoundsPath,
		}
	}

	go func() {
		err := woundsConsumer.Do(signature.Container, vctx.Wounds)
		if err != nil {
			select {
			case <-cancelled:
				// another error happened, give up
				return
			case errs <- err:
				// great!
				return
			}
		}
		done <- true
	}()

	bytesDone := int64(0)
	onProgress := func(delta int64) {
		atomic.AddInt64(&bytesDone, delta)
		vctx.Consumer.Progress(float64(atomic.LoadInt64(&bytesDone)) / float64(signature.Container.Size))
	}

	// validate dirs and symlinks first
	for dirIndex, dir := range signature.Container.Dirs {
		path := filepath.Join(target, filepath.FromSlash(dir.Path))
		stats, err := os.Lstat(path)
		if err != nil {
			if os.IsNotExist(err) {
				vctx.Wounds <- &Wound{
					Kind:  WoundKind_DIR,
					Index: int64(dirIndex),
				}
				continue
			} else {
				return err
			}
		}

		if !stats.IsDir() {
			vctx.Wounds <- &Wound{
				Kind:  WoundKind_DIR,
				Index: int64(dirIndex),
			}
			continue
		}
	}

	for symlinkIndex, symlink := range signature.Container.Symlinks {
		path := filepath.Join(target, filepath.FromSlash(symlink.Path))
		dest, err := os.Readlink(path)
		if err != nil {
			if os.IsNotExist(err) {
				vctx.Wounds <- &Wound{
					Kind:  WoundKind_SYMLINK,
					Index: int64(symlinkIndex),
				}
				continue
			} else {
				return err
			}
		}

		if dest != filepath.FromSlash(symlink.Dest) {
			vctx.Wounds <- &Wound{
				Kind:  WoundKind_SYMLINK,
				Index: int64(symlinkIndex),
			}
			continue
		}
	}

	fileIndices := make(chan int64)

	for i := 0; i < numWorkers; i++ {
		go vctx.validate(target, signature, fileIndices, done, errs, onProgress, cancelled)
	}

	for fileIndex := range signature.Container.Files {
		fileIndices <- int64(fileIndex)
	}

	close(fileIndices)

	// wait for all workers to finish
	for i := 0; i < numWorkers; i++ {
		select {
		case err := <-errs:
			return err
		case <-done:
			// good!
		}
	}

	close(vctx.Wounds)

	// wait for wounds writer to finish
	select {
	case err := <-errs:
		return err
	case <-done:
		// good!
	}

	return nil
}

type OnProgressFunc func(delta int64)

func (vctx *ValidatorContext) validate(target string, signature *SignatureInfo, fileIndices chan int64,
	done chan bool, errs chan error, onProgress OnProgressFunc, cancelled chan struct{}) {
	targetPool, err := pools.New(signature.Container, target)
	if err != nil {
		errs <- err
		return
	}

	defer func() {
		err = targetPool.Close()
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		done <- true
	}()

	aggregateOut := make(chan *Wound)
	relayDone := make(chan bool)
	go func() {
	relayLoop:
		for w := range aggregateOut {
			select {
			case <-cancelled:
				// cancelled
				break relayLoop
			case vctx.Wounds <- w:
				// sent
			}
		}
		relayDone <- true
	}()

	wounds := AggregateWounds(aggregateOut, MaxWoundSize)
	defer func() {
		close(wounds)
		<-relayDone
	}()

	validatingPool := &ValidatingPool{
		Pool:      nullpool.New(signature.Container),
		Container: signature.Container,
		Signature: signature,

		Wounds: wounds,
	}

workLoop:
	for fileIndex := range fileIndices {
		file := signature.Container.Files[fileIndex]

		var reader io.Reader
		reader, err = targetPool.GetReader(fileIndex)
		if err != nil {
			if os.IsNotExist(err) {
				// whole file is missing
				wound := &Wound{
					Kind:  WoundKind_FILE,
					Index: fileIndex,
					Start: 0,
					End:   file.Size,
				}
				onProgress(file.Size)

				select {
				case wounds <- wound:
				case <-cancelled:
					break workLoop
				}
				continue workLoop
			} else {
				errs <- err
				return
			}
		}

		var writer io.WriteCloser
		writer, err = validatingPool.GetWriter(fileIndex)
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		lastCount := int64(0)
		countingWriter := counter.NewWriterCallback(func(count int64) {
			delta := count - lastCount
			onProgress(delta)
			lastCount = count
		}, writer)

		var writtenBytes int64
		writtenBytes, err = io.Copy(countingWriter, reader)
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		err = writer.Close()
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		if writtenBytes != file.Size {
			onProgress(file.Size - writtenBytes)
			wound := &Wound{
				Kind:  WoundKind_FILE,
				Index: fileIndex,
				Start: writtenBytes,
				End:   file.Size,
			}

			select {
			case wounds <- wound:
			case <-cancelled:
				break workLoop
			}
		}
	}
}

func AssertValid(target string, signature *SignatureInfo) error {
	vctx := &ValidatorContext{
		FailFast: true,
		Consumer: &state.Consumer{},
	}

	err := vctx.Validate(target, signature)
	if err != nil {
		return err
	}

	return nil
}

package pwr

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/sync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

var (
	// ErrMalformedPatch is returned when a patch could not be parsed
	ErrMalformedPatch = errors.New("malformed patch")

	// ErrIncompatiblePatch is returned when a patch but parsing
	// and applying it is unsupported (e.g. it's a newer version of the format)
	ErrIncompatiblePatch = errors.New("unsupported patch")
)

type ApplyContext struct {
	Consumer *StateConsumer

	TargetPath string
	OutputPath string
	InPlace    bool

	TargetContainer *tlc.Container
	SourceContainer *tlc.Container

	TouchedFiles int
}

// ApplyPatch reads a patch, parses it, and generates the new file tree
func (actx *ApplyContext) ApplyPatch(patchReader io.Reader) error {
	rawPatchWire := wire.NewReadContext(patchReader)
	err := rawPatchWire.ExpectMagic(PatchMagic)
	if err != nil {
		return err
	}

	header := &PatchHeader{}
	err = rawPatchWire.ReadMessage(header)
	if err != nil {
		return fmt.Errorf("while reading message: %s", err)
	}

	patchWire, err := UncompressWire(rawPatchWire, header.Compression)
	if err != nil {
		return err
	}

	targetContainer := &tlc.Container{}
	err = patchWire.ReadMessage(targetContainer)
	if err != nil {
		return err
	}
	actx.TargetContainer = targetContainer

	sourceContainer := &tlc.Container{}
	err = patchWire.ReadMessage(sourceContainer)
	if err != nil {
		return err
	}
	actx.SourceContainer = sourceContainer

	targetPool := targetContainer.NewFilePool(actx.TargetPath)

	sourceContainer.Prepare(actx.OutputPath)
	outputPool := sourceContainer.NewFilePool(actx.OutputPath)

	sctx := mksync()
	sh := &SyncHeader{}

	fileOffset := int64(0)
	sourceBytes := sourceContainer.Size
	onSourceWrite := func(count int64) {
		actx.Consumer.Progress(float64(fileOffset+count) / float64(sourceBytes))
	}

	for fileIndex, f := range sourceContainer.Files {
		actx.Consumer.ProgressLabel(f.Path)
		actx.Consumer.Debug(f.Path)
		fileOffset = f.Offset

		sh.Reset()
		err := patchWire.ReadMessage(sh)
		if err != nil {
			return err
		}

		if sh.FileIndex != int64(fileIndex) {
			fmt.Printf("expected fileIndex = %d, got fileIndex %d\n", fileIndex, sh.FileIndex)
			return ErrMalformedPatch
		}

		ops := make(chan sync.Operation)
		errc := make(chan error, 1)

		go readOps(patchWire, ops, errc)

		bytesWritten, err := lazilyPatchFile(sctx, targetPool, outputPool, sh.FileIndex, onSourceWrite, ops, actx.InPlace)
		if err != nil {
			return err
		}

		err = <-errc
		if err != nil {
			return fmt.Errorf("while reading patch: %s", err.Error())
		}

		if bytesWritten >= 0 {
			actx.TouchedFiles++
			if bytesWritten != f.Size {
				return fmt.Errorf("%s: expected to write %d bytes, wrote %d bytes", f.Path, f.Size, bytesWritten)
			}
		}
	}

	return nil
}

func lazilyPatchFile(sctx *sync.SyncContext, targetPool *tlc.ContainerFilePool, outputPool *tlc.ContainerFilePool,
	fileIndex int64, onSourceWrite counter.CountCallback, ops chan sync.Operation, inplace bool) (written int64, err error) {

	var realops chan sync.Operation

	errs := make(chan error)
	first := true
	noop := false

	for op := range ops {
		if first {
			first = false

			if op.Type == sync.OpBlockRange && op.BlockIndex == 0 {
				// fmt.Printf("blockspan %d\n", op.BlockSpan)
				outputSize := outputPool.GetSize(fileIndex)
				numOutputBlocks := numBlocks(outputSize)

				if inplace &&
					op.BlockSpan == numOutputBlocks &&
					outputSize == targetPool.GetSize(op.FileIndex) &&
					outputPool.GetRelativePath(fileIndex) == targetPool.GetRelativePath(op.FileIndex) {
					// fmt.Printf("no-op!\n")
					noop = true
				} else {
					// fmt.Printf("%d vs %d, %d vs %d, %s vs %s (outputsize = %d)\n",
					// 	op.BlockSpan, numOutputBlocks,
					// 	outputSize, targetPool.GetSize(op.FileIndex),
					// 	outputPool.GetRelativePath(fileIndex), targetPool.GetRelativePath(op.FileIndex),
					// 	outputSize)
				}
			}

			if noop {
				go func() {
					written = -1
					errs <- nil
				}()
			} else {
				realops = make(chan sync.Operation)

				outputPath := outputPool.GetPath(fileIndex)
				writer, err := os.Create(outputPath)
				if err != nil {
					return 0, err
				}

				writeCounter := counter.NewWriterCallback(onSourceWrite, writer)

				go func() {
					err := sctx.ApplyPatch(writeCounter, targetPool, realops)
					if err != nil {
						errs <- err
						return
					}

					err = writer.Close()
					if err != nil {
						errs <- err
						return
					}

					written = writeCounter.Count()
					errs <- nil
				}()
			}
		}

		if !noop {
			realops <- op
		}
	}

	if !noop {
		close(realops)
	}

	err = <-errs
	if err != nil {
		return 0, err
	}

	return
}

func readOps(rc *wire.ReadContext, ops chan sync.Operation, errc chan error) {
	defer close(ops)
	totalOps := 0
	opsCount := []int{0, 0, 0}
	opsBytes := []int64{0, 0, 0}

	rop := &SyncOp{}
	sendOp := func(op sync.Operation) {
		totalOps++
		opsCount[op.Type]++
		ops <- op
	}

	readingOps := true
	for readingOps {
		rop.Reset()
		err := rc.ReadMessage(rop)
		if err != nil {
			errc <- fmt.Errorf("while reading op message: %s", err)
			return
		}

		var op = sync.Operation{}

		switch rop.Type {
		case SyncOp_BLOCK_RANGE:
			sendOp(sync.Operation{
				Type:       sync.OpBlockRange,
				FileIndex:  rop.FileIndex,
				BlockIndex: rop.BlockIndex,
				BlockSpan:  rop.BlockSpan,
			})
			opsBytes[op.Type] += int64(BlockSize) * int64(op.BlockSpan)

		case SyncOp_DATA:
			sendOp(sync.Operation{
				Type: sync.OpData,
				Data: rop.Data,
			})
			opsBytes[op.Type] += int64(len(rop.Data))

		default:
			switch rop.Type {
			case SyncOp_HEY_YOU_DID_IT:
				readingOps = false
			default:
				fmt.Printf("unrecognized rop type %d\n", rop.Type)
				errc <- ErrMalformedPatch
				return
			}
		}
	}

	errc <- nil
}

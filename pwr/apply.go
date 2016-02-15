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

	TargetContainer *tlc.Container
	SourceContainer *tlc.Container
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
		actx.Consumer.Progress(100.0 * float64(fileOffset+count) / float64(sourceBytes))
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

		fullPath := outputPool.GetPath(sh.FileIndex)
		writer, err := os.Create(fullPath)
		if err != nil {
			return err
		}

		writeCounter := counter.NewWriterCallback(onSourceWrite, writer)

		err = sctx.ApplyPatch(writeCounter, targetPool, ops)
		if err != nil {
			return err
		}

		err = <-errc
		if err != nil {
			return fmt.Errorf("while reading patch: %s", err.Error())
		}

		err = writer.Close()
		if err != nil {
			return err
		}

		if writeCounter.Count() != f.Size {
			return fmt.Errorf("%s: expected to write %d bytes, wrote %d bytes", f.Path, f.Size, writeCounter.Count())
		}
	}

	return nil
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
		case SyncOp_BLOCK:
			sendOp(sync.Operation{
				Type:       sync.OpBlock,
				FileIndex:  rop.FileIndex,
				BlockIndex: rop.BlockIndex,
			})
			opsBytes[op.Type] += int64(BlockSize)

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

	// fmt.Printf("totalOps: %d\n", totalOps)
	// for i, label := range []string{"block", "block-range", "data"} {
	// 	fmt.Printf("%10s = %s in %d ops\n", label, humanize.Bytes(uint64(opsBytes[i])), opsCount[i])
	// }
	// fmt.Printf("-----------------------\n")

	errc <- nil
}

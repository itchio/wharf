package pwr

import (
	"fmt"
	"io"
	"log"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
	"github.com/itchio/wharf/wsync"
)

type RediffContext struct {
	SourcePool wsync.Pool
	TargetPool wsync.Pool
}

// TODO: split into AnalyzePatch and OptimizePatch - reading the patch twice
// to save on RAM
func (rc *RediffContext) OptimizePatch(patchReader io.Reader, patchWriter io.Writer) error {
	var err error

	if rc.SourcePool == nil {
		return errors.Wrap(fmt.Errorf("SourcePool cannot be nil"), 1)
	}

	if rc.TargetPool == nil {
		return errors.Wrap(fmt.Errorf("TargetPool cannot be nil"), 1)
	}

	rctx := wire.NewReadContext(patchReader)
	wctx := wire.NewWriteContext(patchWriter)

	err = wctx.WriteMagic(PatchMagic)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	err = rctx.ExpectMagic(PatchMagic)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	ph := &PatchHeader{}
	err = rctx.ReadMessage(ph)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	wph := &PatchHeader{
		Compression: &CompressionSettings{
			Algorithm: CompressionAlgorithm_ZSTD,
			Quality:   9,
		},
	}
	err = wctx.WriteMessage(wph)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	rctx, err = DecompressWire(rctx, ph.Compression)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	wctx, err = CompressWire(wctx, wph.Compression)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	targetContainer := &tlc.Container{}
	err = rctx.ReadMessage(targetContainer)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	err = wctx.WriteMessage(targetContainer)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	sourceContainer := &tlc.Container{}
	err = rctx.ReadMessage(sourceContainer)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	err = wctx.WriteMessage(sourceContainer)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	opBuffer := []*SyncOp{}

	for _, f := range sourceContainer.Files {
		totalBytes := f.Size
		bytesReusedPerFileIndex := make(map[int64]int64)
		opBuffer = opBuffer[:0]
		readingOps := true

		for readingOps {
			rop := &SyncOp{}
			err = rctx.ReadMessage(rop)
			if err != nil {
				return errors.Wrap(err, 1)
			}

			opBuffer = append(opBuffer, rop)

			switch rop.Type {
			case SyncOp_BLOCK_RANGE:
				alreadyReused := bytesReusedPerFileIndex[rop.FileIndex]
				lastBlockIndex := rop.BlockIndex + rop.BlockSpan
				targetFile := targetContainer.Files[rop.FileIndex]
				lastBlockSize := ComputeBlockSize(targetFile.Size, lastBlockIndex)
				otherBlocksSize := BlockSize*rop.BlockSpan - 1

				bytesReusedPerFileIndex[rop.FileIndex] = alreadyReused + otherBlocksSize + lastBlockSize

			case SyncOp_DATA:
				// muffin

			default:
				switch rop.Type {
				case SyncOp_HEY_YOU_DID_IT:
					readingOps = false
				default:
					return errors.Wrap(ErrMalformedPatch, 1)
				}
			}
		}

		// now that we have a full view of the file, if not worth bsdiffing, just copy operations
		for numBytes, fileIndex := range bytesReusedPerFileIndex {
			log.Printf("%d/%d bytes come from file %s", numBytes, totalBytes, targetContainer.Files[fileIndex])
		}
	}

	err = wctx.Close()
	if err != nil {
		return errors.Wrap(err, 1)
	}

	return nil
}

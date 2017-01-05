package pwr

import (
	"fmt"
	"io"

	humanize "github.com/dustin/go-humanize"
	"github.com/go-errors/errors"
	"github.com/golang/protobuf/proto"
	"github.com/itchio/wharf/bsdiff"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
	"github.com/itchio/wharf/wsync"
)

// FileOrigin maps a target's file index to how many bytes it
// contribute to a given source file
type FileOrigin map[int64]int64

// DiffMappings stores correspondances between files - source files are mapped
// to the target file that has the most blocks in common, or has the same name
type DiffMappings map[int64]*DiffMapping

type DiffMapping struct {
	TargetIndex int64
	NumBytes    int64
}

func (dm DiffMappings) ToString(sourceContainer tlc.Container, targetContainer tlc.Container) string {
	s := ""
	for sourceIndex, diffMapping := range dm {
		s += fmt.Sprintf("%s <- %s (%s in common)\n",
			sourceContainer.Files[sourceIndex].Path,
			targetContainer.Files[diffMapping.TargetIndex].Path,
			humanize.IBytes(uint64(diffMapping.NumBytes)),
		)
	}
	return s
}

type RediffContext struct {
	SourcePool wsync.Pool
	TargetPool wsync.Pool

	// optional
	SuffixSortConcurrency int
	Compression           *CompressionSettings
	Consumer              *state.Consumer

	// internal
	DiffMappings DiffMappings
}

func (rc *RediffContext) AnalyzePatch(patchReader io.Reader) error {
	var err error

	rctx := wire.NewReadContext(patchReader)

	err = rctx.ExpectMagic(PatchMagic)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	ph := &PatchHeader{}
	err = rctx.ReadMessage(ph)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	rctx, err = DecompressWire(rctx, ph.Compression)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	targetContainer := &tlc.Container{}
	err = rctx.ReadMessage(targetContainer)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	sourceContainer := &tlc.Container{}
	err = rctx.ReadMessage(sourceContainer)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	rop := &SyncOp{}

	targetPathsToIndex := make(map[string]int64)
	for targetFileIndex, file := range targetContainer.Files {
		targetPathsToIndex[file.Path] = int64(targetFileIndex)
	}

	rc.DiffMappings = make(DiffMappings)

	var doneBytes int64

	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		rc.Consumer.ProgressLabel(sourceFile.Path)
		rc.Consumer.Progress(float64(doneBytes) / float64(sourceContainer.Size))

		bytesReusedPerFileIndex := make(FileOrigin)
		readingOps := true

		for readingOps {
			rop.Reset()
			err = rctx.ReadMessage(rop)
			if err != nil {
				return errors.Wrap(err, 1)
			}

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

		var diffMapping *DiffMapping

		// now that we have a full view of the file, if not worth bsdiffing, just copy operations
		for targetFileIndex, numBytes := range bytesReusedPerFileIndex {
			targetFile := targetContainer.Files[targetFileIndex]
			// first, better, or equal with same name (prefer natural mappings)
			if diffMapping == nil || numBytes > diffMapping.NumBytes || (numBytes == diffMapping.NumBytes && targetFile.Path == sourceFile.Path) {
				diffMapping = &DiffMapping{
					TargetIndex: targetFileIndex,
					NumBytes:    numBytes,
				}
			}
		}

		if diffMapping != nil {
			rc.DiffMappings[int64(sourceFileIndex)] = diffMapping
		}

		doneBytes += sourceFile.Size
	}

	return nil
}

func (rc *RediffContext) OptimizePatch(patchReader io.Reader, patchWriter io.Writer) error {
	var err error

	if rc.SourcePool == nil {
		return errors.Wrap(fmt.Errorf("SourcePool cannot be nil"), 1)
	}

	if rc.TargetPool == nil {
		return errors.Wrap(fmt.Errorf("TargetPool cannot be nil"), 1)
	}

	if rc.DiffMappings == nil {
		return errors.Wrap(fmt.Errorf("AnalyzePatch must be called before OptimizePatch"), 1)
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

	compression := rc.Compression
	if compression == nil {
		compression = defaultRediffCompressionSettings()
	}

	wph := &PatchHeader{
		Compression: compression,
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

	rop := &SyncOp{}

	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		diffMapping := rc.DiffMappings[int64(sourceFileIndex)]
		readingOps := true

		if diffMapping == nil {
			fmt.Printf("Copying data...\n")
			for readingOps {
				rop.Reset()
				err = rctx.ReadMessage(rop)
				if err != nil {
					return errors.Wrap(err, 1)
				}

				err = wctx.WriteMessage(rop)
				if err != nil {
					return errors.Wrap(err, 1)
				}

				if rop.Type == SyncOp_HEY_YOU_DID_IT {
					readingOps = false
				}
			}
		} else {
			fmt.Printf("Performing bsdiff...\n")
			for readingOps {
				// throw away old ops
				err = rctx.ReadMessage(rop)
				if err != nil {
					return errors.Wrap(err, 1)
				}

				if rop.Type == SyncOp_HEY_YOU_DID_IT {
					readingOps = false
				}

				// then bsdiff
				dc := &bsdiff.DiffContext{
					SuffixSortConcurrency: rc.SuffixSortConcurrency,

					MeasureMem: true,
				}

				sourceFileReader, err := rc.SourcePool.GetReader(int64(sourceFileIndex))
				if err != nil {
					return errors.Wrap(err, 1)
				}

				targetFileReader, err := rc.TargetPool.GetReader(diffMapping.TargetIndex)
				if err != nil {
					return errors.Wrap(err, 1)
				}

				rc.Consumer.ProgressLabel(sourceFile.Path)

				writeMessage := func(message proto.Message) error {
					if bsdc, ok := message.(*bsdiff.Control); ok {
						rc.Consumer.Infof("Writing message with %d bytes of Add, %d of Copy, Seek %d and Eof %v",
							len(bsdc.Add), len(bsdc.Copy), bsdc.Seek, bsdc.Eof)
					} else {
						rc.Consumer.Infof("Writing non-bsdc message")
					}
					err := wctx.WriteMessage(message)
					return err
				}

				err = dc.Do(targetFileReader, sourceFileReader, writeMessage, rc.Consumer)
				if err != nil {
					return errors.Wrap(err, 1)
				}

				err = wctx.WriteMessage(&SyncOp{
					Type: SyncOp_HEY_YOU_DID_IT,
				})
				if err != nil {
					return errors.Wrap(err, 1)
				}
			}
		}
	}

	err = wctx.Close()
	if err != nil {
		return errors.Wrap(err, 1)
	}

	return nil
}

func defaultRediffCompressionSettings() *CompressionSettings {
	return &CompressionSettings{
		Algorithm: CompressionAlgorithm_ZSTD,
		Quality:   9,
	}
}

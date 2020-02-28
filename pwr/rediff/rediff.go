package rediff

import (
	"fmt"
	"io"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/itchio/headway/state"
	"github.com/itchio/headway/united"
	"github.com/itchio/lake"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/savior"
	"github.com/itchio/wharf/bsdiff"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/wire"
	"github.com/pkg/errors"
)

// FileOrigin maps a target's file index to how many bytes it
// contribute to a given source file
type FileOrigin map[int64]int64

// A DiffMapping is a pair of files that have similar contents (blocks in common)
// or equal paths, and which are good candidates for bsdiffing
type DiffMapping struct {
	TargetIndex int64
	NumBytes    int64
}

// DiffMappings contains one diff mapping for each pair of files to be bsdiff'd
type DiffMappings map[int64]*DiffMapping

// ToString returns a human-readable representation of all diff mappings,
// which gives an overview of how files changed.
func (dm DiffMappings) ToString(sourceContainer tlc.Container, targetContainer tlc.Container) string {
	s := ""
	for sourceIndex, diffMapping := range dm {
		s += fmt.Sprintf("%s <- %s (%s in common)\n",
			sourceContainer.Files[sourceIndex].Path,
			targetContainer.Files[diffMapping.TargetIndex].Path,
			united.FormatBytes(diffMapping.NumBytes),
		)
	}
	return s
}

// Context holds options for the rediff process, along with
// some state.
type context struct {
	params Params

	// set after analyze
	targetContainer *tlc.Container
	sourceContainer *tlc.Container
	diffMappings    DiffMappings
}

type Context interface {
	GetTargetContainer() *tlc.Container
	GetSourceContainer() *tlc.Container
	GetDiffMappings() DiffMappings
	Partitions() int
	Optimize(params OptimizeParams) error
}

var _ Context = (*context)(nil)

type Params struct {
	// PatchReader is a source used twice: to find diff mappings, and then again
	// to copy rsync operations for files that we won't optimize.
	// rediff.Context is in charge of resuming, since it uses it twice.
	PatchReader savior.SeekSource

	// RediffSizeLimit (optional) is the maximum size of a file we'll attempt to rediff.
	// If a file is larger than that, ops will just be copied.
	RediffSizeLimit int64

	// optional
	SuffixSortConcurrency int
	// optional
	Partitions int
	// optional
	Compression *pwr.CompressionSettings
	// optional
	Consumer *state.Consumer
	// optional
	BsdiffStats *bsdiff.DiffStats
	// optional
	ForceMapAll bool
	// optional
	MeasureMem bool
}

type OptimizeParams struct {
	TargetPool lake.Pool
	SourcePool lake.Pool

	PatchWriter io.Writer
}

const DefaultRediffSizeLimit = 4 * 1024 * 1024 * 1024 // 4GB

// NewContext initializes the diffing process, it also analyzes the patch
// to find diff mapping so that it's ready for optimization
func NewContext(params Params) (Context, error) {
	err := validation.ValidateStruct(&params,
		validation.Field(&params.PatchReader, validation.Required),
	)
	if err != nil {
		return nil, err
	}

	// apply default params
	if params.RediffSizeLimit == 0 {
		params.RediffSizeLimit = DefaultRediffSizeLimit
	}

	cx := &context{
		params: params,
	}
	err = cx.analyzePatch()
	if err != nil {
		return nil, err
	}

	return cx, nil
}

// AnalyzePatch parses a non-optimized patch, looking for good bsdiff'ing candidates
// and building DiffMappings.
func (cx *context) analyzePatch() error {
	var err error
	consumer := cx.params.Consumer

	_, err = cx.params.PatchReader.Resume(nil)
	if err != nil {
		return err
	}

	rctx := wire.NewReadContext(cx.params.PatchReader)

	err = rctx.ExpectMagic(pwr.PatchMagic)
	if err != nil {
		return err
	}

	ph := &pwr.PatchHeader{}
	err = rctx.ReadMessage(ph)
	if err != nil {
		return err
	}

	rctx, err = pwr.DecompressWire(rctx, ph.Compression)
	if err != nil {
		return errors.WithStack(err)
	}

	targetContainer := &tlc.Container{}
	err = rctx.ReadMessage(targetContainer)
	if err != nil {
		return errors.WithStack(err)
	}
	cx.targetContainer = targetContainer

	sourceContainer := &tlc.Container{}
	err = rctx.ReadMessage(sourceContainer)
	if err != nil {
		return errors.WithStack(err)
	}
	cx.sourceContainer = sourceContainer

	rop := &pwr.SyncOp{}

	targetPathsToIndex := make(map[string]int64)
	for targetFileIndex, file := range targetContainer.Files {
		targetPathsToIndex[file.Path] = int64(targetFileIndex)
	}

	cx.diffMappings = make(DiffMappings)

	var doneBytes int64

	sh := &pwr.SyncHeader{}

	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		sh.Reset()
		err = rctx.ReadMessage(sh)
		if err != nil {
			return errors.WithStack(err)
		}

		if sh.FileIndex != int64(sourceFileIndex) {
			return errors.WithStack(fmt.Errorf("Malformed patch, expected index %d, got %d", sourceFileIndex, sh.FileIndex))
		}

		consumer.ProgressLabel(sourceFile.Path)
		consumer.Progress(float64(doneBytes) / float64(sourceContainer.Size))

		bytesReusedPerFileIndex := make(FileOrigin)
		readingOps := true
		var numBlockRange int64
		var numData int64

		for readingOps {
			rop.Reset()
			err = rctx.ReadMessage(rop)
			if err != nil {
				return errors.WithStack(err)
			}

			switch rop.Type {
			case pwr.SyncOp_BLOCK_RANGE:
				numBlockRange++
				alreadyReused := bytesReusedPerFileIndex[rop.FileIndex]
				lastBlockIndex := rop.BlockIndex + rop.BlockSpan
				targetFile := targetContainer.Files[rop.FileIndex]
				lastBlockSize := pwr.ComputeBlockSize(targetFile.Size, lastBlockIndex)
				otherBlocksSize := pwr.BlockSize*rop.BlockSpan - 1

				bytesReusedPerFileIndex[rop.FileIndex] = alreadyReused + otherBlocksSize + lastBlockSize

			case pwr.SyncOp_DATA:
				numData++

			default:
				switch rop.Type {
				case pwr.SyncOp_HEY_YOU_DID_IT:
					readingOps = false
				default:
					return errors.Errorf("Malformed patch, unknown sync type op %d", rop.Type)
				}
			}
		}

		if sourceFile.Size == 0 && !cx.params.ForceMapAll {
			// don't need to bsdiff newly-empty files
		} else if numBlockRange == 1 && numData == 0 && !cx.params.ForceMapAll {
			// transpositions (renames, etc.) don't need bsdiff'ing :)
		} else {
			var diffMapping *DiffMapping

			for targetFileIndex, numBytes := range bytesReusedPerFileIndex {
				targetFile := targetContainer.Files[targetFileIndex]
				// first, better, or equal target file with same name (prefer natural mappings)
				if diffMapping == nil || numBytes > diffMapping.NumBytes || (numBytes == diffMapping.NumBytes && targetFile.Path == sourceFile.Path) {
					diffMapping = &DiffMapping{
						TargetIndex: targetFileIndex,
						NumBytes:    numBytes,
					}
				}
			}

			if diffMapping == nil {
				// even without any common blocks, bsdiff might still be worth it
				// if the file is named the same
				if samePathTargetFileIndex, ok := targetPathsToIndex[sourceFile.Path]; ok {
					targetFile := targetContainer.Files[samePathTargetFileIndex]

					// don't take into account files that were 0 bytes (it happens). bsdiff won't like that.
					if targetFile.Size > 0 {
						diffMapping = &DiffMapping{
							TargetIndex: samePathTargetFileIndex,
							NumBytes:    0,
						}
					}
				}
			}

			if sourceFile.Size > cx.params.RediffSizeLimit {
				// source file is too large, skip rediff
				diffMapping = nil
			}

			if diffMapping != nil {
				targetFile := targetContainer.Files[diffMapping.TargetIndex]
				if targetFile.Size > cx.params.RediffSizeLimit {
					// target file is too large, skip rediff
					diffMapping = nil
				}
			}

			if diffMapping != nil {
				cx.diffMappings[int64(sourceFileIndex)] = diffMapping
			}
		}

		doneBytes += sourceFile.Size
	}

	return nil
}

// OptimizePatch uses the information computed by AnalyzePatch to write a new version of
// the patch, but with bsdiff instead of rsync diffs for each DiffMapping.
func (cx *context) Optimize(params OptimizeParams) error {
	consumer := cx.params.Consumer

	err := validation.ValidateStruct(&params,
		validation.Field(&params.SourcePool, validation.Required),
		validation.Field(&params.TargetPool, validation.Required),
		validation.Field(&params.PatchWriter, validation.Required),
	)
	if err != nil {
		return err
	}

	_, err = cx.params.PatchReader.Resume(nil)
	if err != nil {
		return err
	}

	rctx := wire.NewReadContext(cx.params.PatchReader)
	wctx := wire.NewWriteContext(params.PatchWriter)

	err = wctx.WriteMagic(pwr.PatchMagic)
	if err != nil {
		return errors.WithStack(err)
	}

	err = rctx.ExpectMagic(pwr.PatchMagic)
	if err != nil {
		return errors.WithStack(err)
	}

	ph := &pwr.PatchHeader{}
	err = rctx.ReadMessage(ph)
	if err != nil {
		return errors.WithStack(err)
	}

	compression := cx.params.Compression
	if compression == nil {
		compression = defaultRediffCompressionSettings()
	}

	wph := &pwr.PatchHeader{
		Compression: compression,
	}
	err = wctx.WriteMessage(wph)
	if err != nil {
		return errors.WithStack(err)
	}

	rctx, err = pwr.DecompressWire(rctx, ph.Compression)
	if err != nil {
		return errors.WithStack(err)
	}

	wctx, err = pwr.CompressWire(wctx, wph.Compression)
	if err != nil {
		return errors.WithStack(err)
	}

	targetContainer := &tlc.Container{}
	err = rctx.ReadMessage(targetContainer)
	if err != nil {
		return errors.WithStack(err)
	}

	err = wctx.WriteMessage(targetContainer)
	if err != nil {
		return errors.WithStack(err)
	}

	sourceContainer := &tlc.Container{}
	err = rctx.ReadMessage(sourceContainer)
	if err != nil {
		return errors.WithStack(err)
	}

	err = wctx.WriteMessage(sourceContainer)
	if err != nil {
		return errors.WithStack(err)
	}

	sh := &pwr.SyncHeader{}
	bh := &pwr.BsdiffHeader{}
	rop := &pwr.SyncOp{}

	bdc := &bsdiff.DiffContext{
		SuffixSortConcurrency: cx.params.SuffixSortConcurrency,
		Partitions:            cx.params.Partitions,
		Stats:                 cx.params.BsdiffStats,
		MeasureMem:            cx.params.MeasureMem,
	}

	bconsumer := &state.Consumer{}

	var biggestSourceFile int64
	var totalRediffSize int64

	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		if _, ok := cx.diffMappings[int64(sourceFileIndex)]; ok {
			if sourceFile.Size > biggestSourceFile {
				biggestSourceFile = sourceFile.Size
			}

			totalRediffSize += sourceFile.Size
		}
	}

	var doneSize int64

	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		sh.Reset()
		err = rctx.ReadMessage(sh)
		if err != nil {
			return errors.WithStack(err)
		}

		if sh.FileIndex != int64(sourceFileIndex) {
			return errors.WithStack(fmt.Errorf("Malformed patch, expected index %d, got %d", sourceFileIndex, sh.FileIndex))
		}

		diffMapping := cx.diffMappings[int64(sourceFileIndex)]

		if diffMapping == nil {
			// if no mapping, just copy ops straight up
			err = wctx.WriteMessage(sh)
			if err != nil {
				return errors.WithStack(err)
			}

			for {
				rop.Reset()
				err = rctx.ReadMessage(rop)
				if err != nil {
					return errors.WithStack(err)
				}

				if rop.Type == pwr.SyncOp_HEY_YOU_DID_IT {
					break
				}

				err = wctx.WriteMessage(rop)
				if err != nil {
					return errors.WithStack(err)
				}
			}
		} else {
			// signal bsdiff start to patcher
			sh.Reset()
			sh.FileIndex = int64(sourceFileIndex)
			sh.Type = pwr.SyncHeader_BSDIFF
			err = wctx.WriteMessage(sh)
			if err != nil {
				return errors.WithStack(err)
			}

			bh.Reset()
			bh.TargetIndex = diffMapping.TargetIndex
			err = wctx.WriteMessage(bh)
			if err != nil {
				return errors.WithStack(err)
			}

			// throw away old ops
			for {
				err = rctx.ReadMessage(rop)
				if err != nil {
					return errors.WithStack(err)
				}

				if rop.Type == pwr.SyncOp_HEY_YOU_DID_IT {
					break
				}
			}

			// then bsdiff
			sourceFileReader, err := params.SourcePool.GetReadSeeker(int64(sourceFileIndex))
			if err != nil {
				return errors.WithStack(err)
			}

			targetFileReader, err := params.TargetPool.GetReadSeeker(diffMapping.TargetIndex)
			if err != nil {
				return errors.WithStack(err)
			}

			consumer.ProgressLabel(fmt.Sprintf(">%s", sourceFile.Path))

			_, err = sourceFileReader.Seek(0, io.SeekStart)
			if err != nil {
				return errors.WithStack(err)
			}

			consumer.ProgressLabel(fmt.Sprintf("<%s", sourceFile.Path))

			_, err = targetFileReader.Seek(0, io.SeekStart)
			if err != nil {
				return errors.WithStack(err)
			}

			consumer.ProgressLabel(fmt.Sprintf("*%s", sourceFile.Path))

			err = bdc.Do(targetFileReader, sourceFileReader, wctx.WriteMessage, bconsumer)
			if err != nil {
				return errors.WithStack(err)
			}

			doneSize += sourceFile.Size
		}

		// and don't forget to indicate success
		rop.Reset()
		rop.Type = pwr.SyncOp_HEY_YOU_DID_IT

		err = wctx.WriteMessage(rop)
		if err != nil {
			return errors.WithStack(err)
		}

		consumer.Progress(float64(doneSize) / float64(totalRediffSize))
	}

	err = wctx.Close()
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (cx *context) Partitions() int {
	return cx.params.Partitions
}

func (cx *context) GetSourceContainer() *tlc.Container {
	return cx.sourceContainer
}

func (cx *context) GetTargetContainer() *tlc.Container {
	return cx.targetContainer
}

func (cx *context) GetDiffMappings() DiffMappings {
	return cx.diffMappings
}

func defaultRediffCompressionSettings() *pwr.CompressionSettings {
	return &pwr.CompressionSettings{
		Algorithm: pwr.CompressionAlgorithm_BROTLI,
		Quality:   9,
	}
}

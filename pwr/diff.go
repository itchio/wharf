package pwr

import (
	"fmt"
	"io"

	"gopkg.in/kothar/brotli-go.v0/enc"

	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/sync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

// ProgressCallback is called periodically to announce the degree of completeness of an operation
type ProgressCallback func(percent float64)

// WriteRecipe outputs a pwr recipe to recipeWriter
func WriteRecipe(
	recipeWriter io.Writer,
	sourceContainer *tlc.Container, sourcePath string,
	targetContainer *tlc.Container, targetSignature []sync.BlockHash,
	onProgress ProgressCallback,
	brotliParams *enc.BrotliParams) ([]sync.BlockHash, error) {

	sourceSignature := make([]sync.BlockHash, 0)

	wc := wire.NewWriteContext(recipeWriter)

	header := &RecipeHeader{}

	header.Compression = &CompressionSettings{
		Algorithm: CompressionAlgorithm_BROTLI,
		Quality:   1,
	}

	err := wc.WriteMessage(header)
	if err != nil {
		return nil, err
	}

	bw := enc.NewBrotliWriter(brotliParams, recipeWriter)
	bwc := wire.NewWriteContext(bw)
	// bwc := wc

	err = bwc.WriteMessage(targetContainer)
	if err != nil {
		return nil, err
	}

	err = bwc.WriteMessage(sourceContainer)
	if err != nil {
		return nil, err
	}

	sourceBytes := sourceContainer.Size
	fileOffset := int64(0)

	onSourceRead := func(count int64) {
		onProgress(100.0 * float64(fileOffset+count) / float64(sourceBytes))
	}

	opsWriter := makeOpsWriter(bwc)

	sctx := mksync()
	blockLibrary := sync.NewBlockLibrary(targetSignature)

	sh := &SyncHeader{}
	delimiter := &SyncOp{}
	delimiter.Type = SyncOp_HEY_YOU_DID_IT

	filePool := sourceContainer.NewFilePool(sourcePath)
	defer filePool.Close()

	for fileIndex, f := range sourceContainer.Files {
		fileOffset = f.Offset

		sh.Reset()
		sh.FileIndex = int64(fileIndex)
		err = bwc.WriteMessage(sh)
		if err != nil {
			return nil, err
		}

		sourceReader, err := filePool.GetReader(int64(fileIndex))
		if err != nil {
			return nil, err
		}

		sourceReaderCounter := counter.NewReaderCallback(onSourceRead, sourceReader)
		err = sctx.ComputeDiff(sourceReaderCounter, blockLibrary, opsWriter)
		if err != nil {
			return nil, err
		}

		err = bwc.WriteMessage(delimiter)
		if err != nil {
			return nil, err
		}
	}

	err = bw.Close()
	if err != nil {
		return nil, err
	}

	return sourceSignature, nil
}

func makeOpsWriter(wc *wire.WriteContext) sync.OperationWriter {
	numOps := 0
	wop := &SyncOp{}

	return func(op sync.Operation) error {
		numOps++
		wop.Reset()

		switch op.Type {
		case sync.OpBlock:
			wop.Type = SyncOp_BLOCK
			wop.BlockIndex = op.BlockIndex

		case sync.OpBlockRange:
			wop.Type = SyncOp_BLOCK_RANGE
			wop.BlockIndex = op.BlockIndex
			wop.BlockSpan = op.BlockSpan

		case sync.OpData:
			wop.Type = SyncOp_DATA
			wop.Data = op.Data

		default:
			return fmt.Errorf("unknown rsync op type: %d", op.Type)
		}

		err := wc.WriteMessage(wop)
		if err != nil {
			return err
		}

		return nil
	}
}

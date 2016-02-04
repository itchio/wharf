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

type DiffContext struct {
	SourceContainer *tlc.Container
	SourcePath      string

	TargetContainer *tlc.Container
	TargetSignature []sync.BlockHash
}

// WriteRecipe outputs a pwr recipe to recipeWriter
func (dctx *DiffContext) WriteRecipe(
	recipeWriter io.Writer,
	onProgress ProgressCallback,
	brotliParams *enc.BrotliParams,
	sourceSignatureWriter sync.SignatureWriter) error {

	wc := wire.NewWriteContext(recipeWriter)

	header := &RecipeHeader{}

	err := wc.WriteMessage(header)
	if err != nil {
		return err
	}

	bw := enc.NewBrotliWriter(brotliParams, recipeWriter)
	bwc := wire.NewWriteContext(bw)
	// bwc := wc

	err = bwc.WriteMessage(dctx.TargetContainer)
	if err != nil {
		return err
	}

	err = bwc.WriteMessage(dctx.SourceContainer)
	if err != nil {
		return err
	}

	sourceBytes := dctx.SourceContainer.Size
	fileOffset := int64(0)

	onSourceRead := func(count int64) {
		onProgress(100.0 * float64(fileOffset+count) / float64(sourceBytes))
	}

	opsWriter := makeOpsWriter(bwc)

	sctx := mksync()
	blockLibrary := sync.NewBlockLibrary(dctx.TargetSignature)

	sh := &SyncHeader{}
	delimiter := &SyncOp{}
	delimiter.Type = SyncOp_HEY_YOU_DID_IT

	filePool := dctx.SourceContainer.NewFilePool(dctx.SourcePath)
	defer filePool.Close()

	for fileIndex, f := range dctx.SourceContainer.Files {
		fileOffset = f.Offset

		sh.Reset()
		sh.FileIndex = int64(fileIndex)
		err = bwc.WriteMessage(sh)
		if err != nil {
			return err
		}

		sourceReader, err := filePool.GetReader(int64(fileIndex))
		if err != nil {
			return err
		}

		pipeReader, pipeWriter := io.Pipe()
		tee := io.TeeReader(sourceReader, pipeWriter)

		signErrors := make(chan error)
		go signFile(sctx, fileIndex, pipeReader, sourceSignatureWriter, signErrors)

		sourceReaderCounter := counter.NewReaderCallback(onSourceRead, tee)
		err = sctx.ComputeDiff(sourceReaderCounter, blockLibrary, opsWriter)
		if err != nil {
			return err
		}

		pipeWriter.Close()
		err = <-signErrors
		if err != nil {
			return err
		}

		err = bwc.WriteMessage(delimiter)
		if err != nil {
			return err
		}
	}

	err = bw.Close()
	if err != nil {
		return err
	}

	return nil
}

func signFile(sctx *sync.SyncContext, fileIndex int, reader io.Reader, writeHash sync.SignatureWriter, errc chan error) {
	errc <- sctx.CreateSignature(int64(fileIndex), reader, writeHash)
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

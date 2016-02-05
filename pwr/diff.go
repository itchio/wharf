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
	sourceSignatureWriter sync.SignatureWriter) error {

	hwc := wire.NewWriteContext(recipeWriter)

	header := &RecipeHeader{
		Compression: &CompressionSettings{
			Algorithm: CompressionAlgorithm_BROTLI,
			Quality:   1,
		},
	}
	err := hwc.WriteMessage(header)
	if err != nil {
		return err
	}

	brotliParams := enc.NewBrotliParams()
	brotliParams.SetQuality(1)
	compressedWriter := enc.NewBrotliWriter(brotliParams, recipeWriter)

	wc := wire.NewWriteContext(compressedWriter)

	err = wc.WriteMessage(dctx.TargetContainer)
	if err != nil {
		return err
	}

	err = wc.WriteMessage(dctx.SourceContainer)
	if err != nil {
		return err
	}

	sourceBytes := dctx.SourceContainer.Size
	fileOffset := int64(0)

	onSourceRead := func(count int64) {
		onProgress(100.0 * float64(fileOffset+count) / float64(sourceBytes))
	}

	opsWriter := makeOpsWriter(wc)

	sctx := mksync()
	blockLibrary := sync.NewBlockLibrary(dctx.TargetSignature)

	sh := &SyncHeader{}
	delimiter := &SyncOp{}
	delimiter.Type = SyncOp_HEY_YOU_DID_IT

	filePool := dctx.SourceContainer.NewFilePool(dctx.SourcePath)
	defer filePool.Close()

	for fileIndex, f := range dctx.SourceContainer.Files {
		fmt.Printf("%s\n", f.Path)
		fileOffset = f.Offset

		sh.Reset()
		sh.FileIndex = int64(fileIndex)
		err = wc.WriteMessage(sh)
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

		err = wc.WriteMessage(delimiter)
		if err != nil {
			return err
		}
	}

	err = compressedWriter.Close()
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

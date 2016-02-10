package pwr

import (
	"fmt"
	"io"

	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/sync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

type DiffContext struct {
	Compression *CompressionSettings
	Consumer    *StateConsumer

	SourceContainer *tlc.Container
	SourcePath      string

	TargetContainer *tlc.Container
	TargetSignature []sync.BlockHash
}

// WriteRecipe outputs a pwr recipe to recipeWriter
func (dctx *DiffContext) WriteRecipe(recipeWriter io.Writer, signatureWriter io.Writer) error {
	if dctx.Compression == nil {
		dctx.Compression = defaultCompressionSettings()
	}

	// signature header
	rawSigWire := wire.NewWriteContext(signatureWriter)
	err := rawSigWire.WriteMagic(signatureMagic)
	if err != nil {
		return err
	}

	err = rawSigWire.WriteMessage(&SignatureHeader{})
	if err != nil {
		return err
	}

	sigWire, err := compressWire(rawSigWire, dctx.Compression)
	if err != nil {
		return err
	}

	err = sigWire.WriteMessage(dctx.SourceContainer)
	if err != nil {
		return err
	}

	// recipe header
	rawRecipeWire := wire.NewWriteContext(recipeWriter)
	err = rawRecipeWire.WriteMagic(recipeMagic)
	if err != nil {
		return err
	}

	header := &RecipeHeader{
		Compression: &CompressionSettings{
			Algorithm: CompressionAlgorithm_BROTLI,
			Quality:   1,
		},
	}

	err = rawRecipeWire.WriteMessage(header)
	if err != nil {
		return err
	}

	recipeWire, err := compressWire(rawRecipeWire, dctx.Compression)
	if err != nil {
		return err
	}

	err = recipeWire.WriteMessage(dctx.TargetContainer)
	if err != nil {
		return err
	}

	err = recipeWire.WriteMessage(dctx.SourceContainer)
	if err != nil {
		return err
	}

	sourceBytes := dctx.SourceContainer.Size
	fileOffset := int64(0)

	onSourceRead := func(count int64) {
		dctx.Consumer.Progress(100.0 * float64(fileOffset+count) / float64(sourceBytes))
	}

	sigWriter := makeSigWriter(sigWire)
	opsWriter := makeOpsWriter(recipeWire)

	syncContext := mksync()
	blockLibrary := sync.NewBlockLibrary(dctx.TargetSignature)

	// re-used messages
	syncHeader := &SyncHeader{}
	syncDelimiter := &SyncOp{
		Type: SyncOp_HEY_YOU_DID_IT,
	}

	filePool := dctx.SourceContainer.NewFilePool(dctx.SourcePath)
	defer filePool.Close()

	for fileIndex, f := range dctx.SourceContainer.Files {
		dctx.Consumer.Debug(f.Path)
		fileOffset = f.Offset

		syncHeader.Reset()
		syncHeader.FileIndex = int64(fileIndex)
		err = recipeWire.WriteMessage(syncHeader)
		if err != nil {
			return err
		}

		sourceReader, err := filePool.GetReader(int64(fileIndex))
		if err != nil {
			return err
		}

		//             / differ
		// source file +
		//             \ signer
		pipeReader, pipeWriter := io.Pipe()
		tee := io.TeeReader(sourceReader, pipeWriter)

		signErrors := make(chan error)
		go signFile(syncContext, fileIndex, pipeReader, sigWriter, signErrors)

		sourceReaderCounter := counter.NewReaderCallback(onSourceRead, tee)
		err = syncContext.ComputeDiff(sourceReaderCounter, blockLibrary, opsWriter)
		if err != nil {
			return err
		}

		pipeWriter.Close()
		err = <-signErrors
		if err != nil {
			return err
		}

		err = recipeWire.WriteMessage(syncDelimiter)
		if err != nil {
			return err
		}
	}

	return nil
}

func signFile(sctx *sync.SyncContext, fileIndex int, reader io.Reader, writeHash sync.SignatureWriter, errc chan error) {
	errc <- sctx.CreateSignature(int64(fileIndex), reader, writeHash)
}

func makeSigWriter(wc *wire.WriteContext) sync.SignatureWriter {
	return func(bl sync.BlockHash) error {
		wc.WriteMessage(&BlockHash{
			WeakHash:   bl.WeakHash,
			StrongHash: bl.StrongHash,
		})
		return nil
	}
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

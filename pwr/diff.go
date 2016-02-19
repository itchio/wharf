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

	ReusedBytes int64
	FreshBytes  int64
}

// WritePatch outputs a pwr patch to patchWriter
func (dctx *DiffContext) WritePatch(patchWriter io.Writer, signatureWriter io.Writer) error {
	if dctx.Compression == nil {
		dctx.Compression = CompressionDefault()
	}

	// signature header
	rawSigWire := wire.NewWriteContext(signatureWriter)
	err := rawSigWire.WriteMagic(SignatureMagic)
	if err != nil {
		return err
	}

	err = rawSigWire.WriteMessage(&SignatureHeader{
		Compression: dctx.Compression,
	})
	if err != nil {
		return err
	}

	sigWire, err := CompressWire(rawSigWire, dctx.Compression)
	if err != nil {
		return err
	}

	err = sigWire.WriteMessage(dctx.SourceContainer)
	if err != nil {
		return err
	}

	// patch header
	rawPatchWire := wire.NewWriteContext(patchWriter)
	err = rawPatchWire.WriteMagic(PatchMagic)
	if err != nil {
		return err
	}

	header := &PatchHeader{
		Compression: dctx.Compression,
	}

	err = rawPatchWire.WriteMessage(header)
	if err != nil {
		return err
	}

	patchWire, err := CompressWire(rawPatchWire, dctx.Compression)
	if err != nil {
		return err
	}

	err = patchWire.WriteMessage(dctx.TargetContainer)
	if err != nil {
		return err
	}

	err = patchWire.WriteMessage(dctx.SourceContainer)
	if err != nil {
		return err
	}

	sourceBytes := dctx.SourceContainer.Size
	fileOffset := int64(0)

	onSourceRead := func(count int64) {
		dctx.Consumer.Progress(float64(fileOffset+count) / float64(sourceBytes))
	}

	sigWriter := makeSigWriter(sigWire)
	opsWriter := makeOpsWriter(patchWire, dctx)

	diffSyncContext := mksync()
	signSyncContext := mksync()
	blockLibrary := sync.NewBlockLibrary(dctx.TargetSignature)

	// re-used messages
	syncHeader := &SyncHeader{}
	syncDelimiter := &SyncOp{
		Type: SyncOp_HEY_YOU_DID_IT,
	}

	filePool := dctx.SourceContainer.NewFilePool(dctx.SourcePath)
	defer filePool.Close()

	for fileIndex, f := range dctx.SourceContainer.Files {
		dctx.Consumer.ProgressLabel(f.Path)
		dctx.Consumer.Debug(f.Path)
		fileOffset = f.Offset

		syncHeader.Reset()
		syncHeader.FileIndex = int64(fileIndex)
		err = patchWire.WriteMessage(syncHeader)
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
		diffReader, diffWriter := io.Pipe()
		signReader, signWriter := io.Pipe()

		done := make(chan bool)
		errs := make(chan error)

		go diffFile(diffSyncContext, blockLibrary, diffReader, opsWriter, errs, done)
		go signFile(signSyncContext, fileIndex, signReader, sigWriter, errs, done)

		go func() {
			defer diffWriter.Close()
			defer signWriter.Close()

			mw := io.MultiWriter(diffWriter, signWriter)

			sourceReadCounter := counter.NewReaderCallback(onSourceRead, sourceReader)
			_, err := io.Copy(mw, sourceReadCounter)
			if err != nil {
				errs <- err
			}
		}()

		// wait until all are done
		// or an error occurs
		for c := 0; c < 2; c++ {
			select {
			case err := <-errs:
				return err
			case <-done:
			}
		}

		err = patchWire.WriteMessage(syncDelimiter)
		if err != nil {
			return err
		}
	}

	patchWire.Close()
	sigWire.Close()

	return nil
}

func diffFile(sctx *sync.SyncContext, blockLibrary *sync.BlockLibrary, reader io.Reader, opsWriter sync.OperationWriter, errs chan error, done chan bool) {
	err := sctx.ComputeDiff(reader, blockLibrary, opsWriter)
	if err != nil {
		errs <- err
	}

	done <- true
}

func signFile(sctx *sync.SyncContext, fileIndex int, reader io.Reader, writeHash sync.SignatureWriter, errs chan error, done chan bool) {
	err := sctx.CreateSignature(int64(fileIndex), reader, writeHash)
	if err != nil {
		errs <- err
	}

	done <- true
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

func makeOpsWriter(wc *wire.WriteContext, dctx *DiffContext) sync.OperationWriter {
	numOps := 0
	wop := &SyncOp{}

	return func(op sync.Operation) error {
		numOps++
		wop.Reset()

		switch op.Type {
		case sync.OpBlock:
			wop.Type = SyncOp_BLOCK
			wop.FileIndex = op.FileIndex
			wop.BlockIndex = op.BlockIndex

			opSize := int64(BlockSize)
			fileSize := dctx.TargetContainer.Files[op.FileIndex].Size

			offset := op.BlockIndex * int64(BlockSize)
			if offset >= fileSize/int64(BlockSize) {
				opSize = fileSize % int64(BlockSize)
			}
			dctx.ReusedBytes += opSize

		case sync.OpBlockRange:
			wop.Type = SyncOp_BLOCK_RANGE
			wop.FileIndex = op.FileIndex
			wop.BlockIndex = op.BlockIndex
			wop.BlockSpan = op.BlockSpan

			fileSize := dctx.TargetContainer.Files[op.FileIndex].Size

			lastBlockSize := int64(BlockSize)
			offset := (op.BlockIndex + op.BlockSpan) * int64(BlockSize)
			if offset >= fileSize/int64(BlockSize) {
				lastBlockSize = fileSize % int64(BlockSize)
				if lastBlockSize == 0 {
					lastBlockSize = int64(BlockSize)
				}
			}

			dctx.ReusedBytes += int64(BlockSize)*(op.BlockSpan-1) + lastBlockSize

		case sync.OpData:
			wop.Type = SyncOp_DATA
			wop.Data = op.Data
			dctx.FreshBytes += int64(len(op.Data))

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

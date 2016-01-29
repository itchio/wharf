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
	sourceContainer *tlc.Container,
	targetContainer *tlc.Container, signature []sync.BlockHash,
	onProgress ProgressCallback,
	brotliParams *enc.BrotliParams) error {

	wc := wire.NewWriteContext(recipeWriter)

	header := &RecipeHeader{}
	header.Version = RecipeHeader_V1

	// header.Compression = RecipeHeader_BROTLI
	header.Compression = RecipeHeader_UNCOMPRESSED
	header.CompressionLevel = 1

	err := wc.WriteMessage(header)
	if err != nil {
		return err
	}

	// bw := enc.NewBrotliWriter(brotliParams, recipeWriter)
	// bwc := wire.NewWriteContext(bw)
	bwc := wc

	writeContainer(bwc, targetContainer)
	writeContainer(bwc, sourceContainer)

	sourceBytes := sourceContainer.Size
	fileOffset := int64(0)

	onSourceRead := func(count int64) {
		onProgress(100.0 * float64(fileOffset+count) / float64(sourceBytes))
	}

	opsWriter := makeOpsWriter(bwc)

	sctx := mksync()
	blockLibrary := sync.NewBlockLibrary(signature)

	sh := &SyncHeader{}
	filePool := sourceContainer.NewFilePool()
	defer filePool.Close()

	for fileIndex, f := range sourceContainer.Files {
		fileOffset = f.Offset

		sh.Reset()
		sh.FileIndex = int64(fileIndex)
		bwc.WriteMessage(sh)

		sourceReader, err := filePool.GetReader(int64(fileIndex))
		if err != nil {
			return err
		}

		sourceReaderCounter := counter.NewReaderCallback(onSourceRead, sourceReader)
		err = sctx.ComputeDiff(sourceReaderCounter, blockLibrary, opsWriter)

		if err != nil {
			return err
		}
	}

	eop := &SyncOp{}
	eop.Type = SyncOp_HEY_YOU_DID_IT
	err = bwc.WriteMessage(eop)
	if err != nil {
		return err
	}

	// err = bw.Close()
	// if err != nil {
	// 	return err
	// }

	return nil
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
			wop.BlockSpan = op.BlockIndex

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

func writeContainer(bwc *wire.WriteContext, container *tlc.Container) error {
	dirs := make([]*Container_Dir, 0, len(container.Dirs))
	for _, d := range container.Dirs {
		dirs = append(dirs, &Container_Dir{
			Path: d.Path,
			Mode: uint32(d.Mode),
		})
	}

	files := make([]*Container_File, 0, len(container.Files))
	for _, f := range container.Files {
		files = append(files, &Container_File{
			Path: f.Path,
			Mode: uint32(f.Mode),
			Size: f.Size,
		})
	}

	symlinks := make([]*Container_Symlink, 0, len(container.Symlinks))
	for _, s := range container.Symlinks {
		symlinks = append(symlinks, &Container_Symlink{
			Path: s.Path,
			Mode: uint32(s.Mode),
			Dest: s.Dest,
		})
	}

	msg := &Container{
		Size:     container.Size,
		Dirs:     dirs,
		Files:    files,
		Symlinks: symlinks,
	}

	return bwc.WriteMessage(msg)
}

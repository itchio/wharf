package pwr

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

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

// ApplyContext holds the state while applying a patch
type ApplyContext struct {
	Consumer *StateConsumer

	TargetPath string
	OutputPath string
	InPlace    bool

	TargetContainer *tlc.Container
	SourceContainer *tlc.Container

	TouchedFiles int
	NoopFiles    int
	DeletedFiles int
}

// ApplyPatch reads a patch, parses it, and generates the new file tree
func (actx *ApplyContext) ApplyPatch(patchReader io.Reader) error {
	actualOutputPath := actx.OutputPath
	if actx.InPlace {
		stagePath := actualOutputPath + "-stage"
		defer os.RemoveAll(stagePath)
		actx.OutputPath = stagePath
	}

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

	patchWire, err := DecompressWire(rawPatchWire, header.Compression)
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

	sourceFileMap := make(map[string]bool)
	var deletedFiles []string
	if actx.InPlace {
		for _, f := range actx.SourceContainer.Files {
			sourceFileMap[f.Path] = true
		}
		for _, s := range actx.SourceContainer.Symlinks {
			sourceFileMap[s.Path] = true
		}
		for _, d := range actx.SourceContainer.Dirs {
			sourceFileMap[d.Path] = true
		}

		for _, f := range actx.TargetContainer.Files {
			if !sourceFileMap[f.Path] {
				deletedFiles = append(deletedFiles, f.Path)
			}
		}
		for _, s := range actx.TargetContainer.Symlinks {
			if !sourceFileMap[s.Path] {
				deletedFiles = append(deletedFiles, s.Path)
			}
		}
		for _, d := range actx.TargetContainer.Dirs {
			if !sourceFileMap[d.Path] {
				deletedFiles = append(deletedFiles, d.Path)
			}
		}
	} else {
		err = sourceContainer.Prepare(actx.OutputPath)
		if err != nil {
			return err
		}
	}
	outputPool := sourceContainer.NewFilePool(actx.OutputPath)

	sctx := mksync()
	sh := &SyncHeader{}

	fileOffset := int64(0)
	sourceBytes := sourceContainer.Size
	onSourceWrite := func(count int64) {
		actx.Consumer.Progress(float64(fileOffset+count) / float64(sourceBytes))
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

		bytesWritten, noop, err := lazilyPatchFile(sctx, targetPool, outputPool, sh.FileIndex, onSourceWrite, ops, actx.InPlace)
		if err != nil {
			return err
		}

		if noop {
			actx.NoopFiles++
		}

		err = <-errc
		if err != nil {
			return fmt.Errorf("while reading patch: %s", err.Error())
		}

		if bytesWritten >= 0 {
			actx.TouchedFiles++
			if bytesWritten != f.Size {
				return fmt.Errorf("%s: expected to write %d bytes, wrote %d bytes", f.Path, f.Size, bytesWritten)
			}
		}
	}

	err = targetPool.Close()
	if err != nil {
		return err
	}

	if actx.InPlace {
		actx.DeletedFiles = len(deletedFiles)

		err := mergeFolders(actualOutputPath, actx.OutputPath)
		if err != nil {
			return fmt.Errorf("in mergeFolders: %s", err.Error())
		}

		err = deleteFiles(actualOutputPath, deletedFiles)
		if err != nil {
			return fmt.Errorf("in deleteFiles: %s", err.Error())
		}
		actx.OutputPath = actualOutputPath
	}

	return nil
}

func mergeFolders(outPath string, stagePath string) error {
	var filter tlc.FilterFunc = func(fi os.FileInfo) bool {
		return true
	}

	stageContainer, err := tlc.Walk(stagePath, filter)
	if err != nil {
		return err
	}

	move := func(path string) error {
		p := filepath.FromSlash(path)
		op := filepath.Join(outPath, p)
		sp := filepath.Join(stagePath, p)

		err := os.Remove(op)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}

		err = os.MkdirAll(filepath.Dir(op), os.FileMode(0755))
		if err != nil {
			return err
		}

		err = os.Rename(sp, op)
		if err != nil {
			return err
		}
		return nil
	}

	for _, f := range stageContainer.Files {
		err := move(f.Path)
		if err != nil {
			return err
		}
	}

	for _, s := range stageContainer.Symlinks {
		err := move(s.Path)
		if err != nil {
			return err
		}
	}

	return nil
}

type byDecreasingLength []string

func (s byDecreasingLength) Len() int {
	return len(s)
}

func (s byDecreasingLength) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byDecreasingLength) Less(i, j int) bool {
	return len(s[j]) < len(s[i])
}

func deleteFiles(outPath string, deletedFiles []string) error {
	sort.Sort(byDecreasingLength(deletedFiles))

	for _, f := range deletedFiles {
		p := filepath.FromSlash(f)
		op := filepath.Join(outPath, p)
		err := os.Remove(op)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}

	return nil
}

func lazilyPatchFile(sctx *sync.Context, targetPool *tlc.ContainerFilePool, outputPool *tlc.ContainerFilePool,
	fileIndex int64, onSourceWrite counter.CountCallback, ops chan sync.Operation, inplace bool) (written int64, noop bool, err error) {

	var realops chan sync.Operation

	errs := make(chan error)
	first := true

	for op := range ops {
		if first {
			first = false

			if op.Type == sync.OpBlockRange && op.BlockIndex == 0 {
				outputSize := outputPool.GetSize(fileIndex)
				numOutputBlocks := numBlocks(outputSize)

				if inplace &&
					op.BlockSpan == numOutputBlocks &&
					outputSize == targetPool.GetSize(op.FileIndex) &&
					outputPool.GetRelativePath(fileIndex) == targetPool.GetRelativePath(op.FileIndex) {
					noop = true
				}
			}

			if noop {
				go func() {
					written = -1
					errs <- nil
				}()
			} else {
				realops = make(chan sync.Operation)

				outputPath := outputPool.GetPath(fileIndex)
				err = os.MkdirAll(filepath.Dir(outputPath), os.FileMode(0755))
				if err != nil {
					return 0, false, err
				}

				var writer io.WriteCloser
				writer, err = os.Create(outputPath)
				if err != nil {
					return 0, false, err
				}

				writeCounter := counter.NewWriterCallback(onSourceWrite, writer)

				go func() {
					err := sctx.ApplyPatch(writeCounter, targetPool, realops)
					if err != nil {
						errs <- err
						return
					}

					err = writer.Close()
					if err != nil {
						errs <- err
						return
					}

					written = writeCounter.Count()
					errs <- nil
				}()
			}
		}

		if !noop {
			realops <- op
		}
	}

	if !noop {
		close(realops)
	}

	err = <-errs
	if err != nil {
		return 0, false, err
	}

	return
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
		case SyncOp_BLOCK_RANGE:
			sendOp(sync.Operation{
				Type:       sync.OpBlockRange,
				FileIndex:  rop.FileIndex,
				BlockIndex: rop.BlockIndex,
				BlockSpan:  rop.BlockSpan,
			})
			opsBytes[op.Type] += int64(BlockSize) * op.BlockSpan

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

	errc <- nil
}

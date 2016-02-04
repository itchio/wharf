package pwr

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/dustin/go-humanize"
	"github.com/itchio/wharf/sync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

var (
	// ErrMalformedRecipe is returned when a recipe could not be parsed
	ErrMalformedRecipe = errors.New("malformed recipe")

	// ErrIncompatibleRecipe is returned when a recipe but parsing
	// and applying it is unsupported (e.g. it's a newer version of the format)
	ErrIncompatibleRecipe = errors.New("unsupported recipe")
)

// ApplyRecipe reads a recipe, parses it, and generates the new file tree
func ApplyRecipe(recipeReader io.Reader, target string, output string, onProgress ProgressCallback) error {
	rc := wire.NewReadContext(recipeReader)

	header := &RecipeHeader{}
	err := rc.ReadMessage(header)
	if err != nil {
		return fmt.Errorf("while reading message: %s", err)
	}

	targetContainer := &tlc.Container{}
	err = rc.ReadMessage(targetContainer)
	if err != nil {
		return err
	}

	sourceContainer := &tlc.Container{}
	err = rc.ReadMessage(sourceContainer)
	if err != nil {
		return err
	}

	targetPool := targetContainer.NewFilePool(target)

	sourceContainer.Prepare(output)
	outputPool := sourceContainer.NewFilePool(output)

	sctx := mksync()
	sh := &SyncHeader{}

	for fileIndex, f := range sourceContainer.Files {
		fmt.Printf("Patching %s\n", f.Path)

		sh.Reset()
		err := rc.ReadMessage(sh)
		if err != nil {
			return err
		}

		if sh.FileIndex != int64(fileIndex) {
			fmt.Printf("expected fileIndex = %d, got fileIndex %d\n", fileIndex, sh.FileIndex)
			return ErrMalformedRecipe
		}

		ops := make(chan sync.Operation)
		errc := make(chan error, 1)
		go readOps(rc, ops, errc)

		fullPath := outputPool.GetPath(sh.FileIndex)
		writer, err := os.Create(fullPath)
		if err != nil {
			return err
		}

		err = sctx.ApplyRecipe(writer, targetPool, ops)
		if err != nil {
			return err
		}

		err = <-errc
		if err != nil {
			return fmt.Errorf("while reading recipe: %s", err.Error())
		}

		writer.Close()
	}

	return nil
}

func readOps(rc *wire.ReadContext, ops chan sync.Operation, errc chan error) {
	defer close(ops)
	totalOps := 0
	opsCount := []int{0, 0, 0}
	opsBytes := []int64{0, 0, 0}

	rop := &SyncOp{}
	op := sync.Operation{}

	readingOps := true
	for readingOps {
		rop.Reset()
		err := rc.ReadMessage(rop)
		if err != nil {
			errc <- fmt.Errorf("while reading op message: %s", err)
			return
		}
		hasOp := true

		switch rop.Type {
		case SyncOp_BLOCK:
			op.Type = sync.OpBlock
			op.BlockIndex = rop.BlockIndex
			opsBytes[op.Type] += int64(BlockSize)

		case SyncOp_BLOCK_RANGE:
			op.Type = sync.OpBlockRange
			op.BlockIndex = rop.BlockIndex
			op.BlockSpan = rop.BlockSpan
			opsBytes[op.Type] += int64(BlockSize) * int64(op.BlockSpan)

		case SyncOp_DATA:
			op.Type = sync.OpData
			op.Data = rop.Data
			opsBytes[op.Type] += int64(len(op.Data))

		default:
			hasOp = false
			switch rop.Type {
			case SyncOp_HEY_YOU_DID_IT:
				readingOps = false
			default:
				fmt.Printf("unrecognized rop type %d\n", rop.Type)
				errc <- ErrMalformedRecipe
				return
			}
		}

		if hasOp {
			totalOps++
			opsCount[op.Type]++
			ops <- op
		}
	}

	fmt.Printf("totalOps: %d\n", totalOps)
	for i, label := range []string{"block", "block-range", "data"} {
		fmt.Printf("%10s = %s in %d ops\n", label, humanize.Bytes(uint64(opsBytes[i])), opsCount[i])
	}
	fmt.Printf("-----------------------\n")

	errc <- nil
}

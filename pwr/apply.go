package pwr

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/kothar/brotli-go.v0/dec"

	"github.com/itchio/wharf/rsync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

var (
	ErrUnknownRsyncOperation = errors.New("Unknown RSync operation")
)

func ApplyRecipe(recipeReader io.Reader, target string, output string, onProgress ProgressCallback) error {
	fmt.Printf("applyRecipe\n")
	rc := wire.NewReadContext(recipeReader)

	header := &RecipeHeader{}
	err := rc.ReadMessage(header)
	if err != nil {
		return fmt.Errorf("while reading message: %s", err)
	}

	fmt.Printf("got header\n")

	switch header.Version {
	case RecipeHeader_V1:
		// muffin
	default:
		return ErrUnknownRecipeVersion
	}

	var decompressReader io.Reader
	switch header.Compression {
	case RecipeHeader_UNCOMPRESSED:
		decompressReader = recipeReader
	case RecipeHeader_BROTLI:
		decompressReader = dec.NewBrotliReader(recipeReader)
	default:
		return ErrUnknownCompression
	}

	brc := wire.NewReadContext(decompressReader)

	targetInfo, err := readRepoInfo(brc)
	if err != nil {
		return fmt.Errorf("while reading target info: %s", err)
	}
	fmt.Printf("got targetInfo\n")

	sourceInfo, err := readRepoInfo(brc)
	if err != nil {
		return fmt.Errorf("while reading source info: %s", err)
	}
	fmt.Printf("got sourceInfo\n")

	sourceWriter, err := sourceInfo.NewWriter(output)
	if err != nil {
		return fmt.Errorf("while making source writer: %s", err)
	}

	targetReader := targetInfo.NewReader(target)

	rs := mkrsync()

	ops := make(chan rsync.Operation)
	errc := make(chan error, 1)

	go (func() {
		defer close(ops)
		totalOps := 0
		opsCount := []int{0, 0, 0}
		opsBytes := []int64{0, 0, 0}

		rop := &RsyncOp{}
		op := rsync.Operation{}

		readingOps := true
		for readingOps {
			rop.Reset()
			err = brc.ReadMessage(rop)
			if err != nil {
				errc <- err
				return
			}
			fmt.Printf("got op %d\n", rop.Type)
			hasOp := true

			switch rop.Type {
			case RsyncOp_BLOCK:
				fmt.Printf("block\n")
				op.Type = rsync.OpBlock
				op.BlockIndex = rop.BlockIndex
				opsBytes[op.Type] += int64(sourceInfo.BlockSize)

			case RsyncOp_BLOCK_RANGE:
				fmt.Printf("blockRange\n")
				op.Type = rsync.OpBlockRange
				op.BlockIndex = rop.BlockIndex
				op.BlockIndexEnd = rop.BlockIndexEnd
				opsBytes[op.Type] += int64(sourceInfo.BlockSize) * int64(op.BlockIndexEnd-op.BlockIndex)

			case RsyncOp_DATA:
				fmt.Printf("data\n")
				op.Type = rsync.OpData
				op.Data = rop.Data
				opsBytes[op.Type] += int64(sourceInfo.BlockSize)

			default:
				hasOp = false
				switch rop.Type {
				case RsyncOp_HEY_YOU_DID_IT:
					fmt.Printf("hey you did it!\n")
					readingOps = false
				default:
					errc <- ErrUnknownRsyncOperation
					return
				}
			}

			if hasOp {
				totalOps++
				opsCount[op.Type]++
				ops <- op
			}
		}

		errc <- nil
	})()

	err = rs.ApplyRecipe(sourceWriter, targetReader, ops)
	if err != nil {
		return fmt.Errorf("While applying recipe: %s", err.Error())
	}

	err = <-errc
	if err != nil {
		return fmt.Errorf("While reading recipe: %s", err.Error())
	}

	return nil
}

func readRepoInfo(rc *wire.ReadContext) (*tlc.RepoInfo, error) {
	pi := &RepoInfo{}
	err := rc.ReadMessage(pi)
	if err != nil {
		return nil, err
	}

	ri := &tlc.RepoInfo{
		BlockSize: BLOCK_SIZE,
		NumBlocks: pi.NumBlocks,
		Dirs:      make([]tlc.Dir, 0, len(pi.GetDirs())),
		Files:     make([]tlc.File, 0, len(pi.GetFiles())),
		Symlinks:  make([]tlc.Symlink, 0, len(pi.GetSymlinks())),
	}

	for _, d := range pi.GetDirs() {
		ri.Dirs = append(ri.Dirs, tlc.Dir{
			Path: d.Path,
			Mode: os.FileMode(d.Mode),
		})
	}

	for _, f := range pi.GetFiles() {
		ri.Files = append(ri.Files, tlc.File{
			Path:          f.Path,
			Mode:          os.FileMode(f.Mode),
			Size:          f.Size,
			BlockIndex:    f.BlockIndex,
			BlockIndexEnd: f.BlockIndexEnd,
		})
	}

	for _, l := range pi.GetSymlinks() {
		ri.Symlinks = append(ri.Symlinks, tlc.Symlink{
			Path: l.Path,
			Mode: os.FileMode(l.Mode),
			Dest: l.Dest,
		})
	}

	return ri, nil
}

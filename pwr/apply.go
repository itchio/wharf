package pwr

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/kothar/brotli-go.v0/dec"

	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/rsync"
	"github.com/itchio/wharf/wire"
)

var (
	ErrUnknownRsyncOperation = errors.New("Unknown RSync operation")
)

func apply(recipeReader io.Reader, target string, output string, onProgress ProgressCallback) error {
	rc := wire.NewReadContext(recipeReader)

	header := &RecipeHeader{}
	err := rc.ReadMessage(header)
	if err != nil {
		return err
	}

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
		return err
	}

	sourceInfo, err := readRepoInfo(brc)
	if err != nil {
		return err
	}

	sourceWriter, err := sourceInfo.NewWriter(output)
	if err != nil {
		return err
	}

	targetReader := targetInfo.NewReader(target)

	rs := &rsync.RSync{
		BlockSize: sourceInfo.BlockSize,
	}

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

			switch rop.Type {
			case RsyncOp_HEY_YOU_DID_IT:
				readingOps = false
				break

			case RsyncOp_BLOCK:
				op.Type = rsync.OpBlock
				op.BlockIndex = rop.BlockIndex
				opsBytes[op.Type] += int64(sourceInfo.BlockSize)

			case RsyncOp_BLOCK_RANGE:
				op.Type = rsync.OpBlockRange
				op.BlockIndex = rop.BlockIndex
				op.BlockIndexEnd = rop.BlockIndexEnd
				opsBytes[op.Type] += int64(sourceInfo.BlockSize) * int64(op.BlockIndexEnd-op.BlockIndex)

			case RsyncOp_DATA:
				op.Type = rsync.OpData
				op.Data = rop.Data
				opsBytes[op.Type] += int64(sourceInfo.BlockSize)

			default:
				errc <- ErrUnknownRsyncOperation
				return
			}

			totalOps++
			opsCount[op.Type]++
			ops <- op
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

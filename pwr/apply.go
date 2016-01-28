package pwr

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/crypto/sha3"

	"gopkg.in/kothar/brotli-go.v0/dec"

	"github.com/itchio/wharf/rsync"
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

	switch header.Version {
	case RecipeHeader_V1:
		// muffin
	default:
		return ErrIncompatibleRecipe
	}

	var decompressReader io.Reader
	switch header.Compression {
	case RecipeHeader_UNCOMPRESSED:
		fmt.Printf("Uncompressed formula\n")
		decompressReader = recipeReader
	case RecipeHeader_BROTLI:
		fmt.Printf("Brotli-compressed formula\n")
		decompressReader = dec.NewBrotliReader(recipeReader)
	default:
		return ErrIncompatibleRecipe
	}

	var shakeHash sha3.ShakeHash
	switch header.FullHashType {
	case HashType_SHAKESUM128:
		shakeHash = sha3.NewShake128()
	default:
		return ErrIncompatibleRecipe
	}

	brc := wire.NewReadContext(decompressReader)

	targetInfo, err := readRepoInfo(brc)
	if err != nil {
		return fmt.Errorf("while reading target info: %s", err)
	}

	sourceInfo, err := readRepoInfo(brc)
	if err != nil {
		return fmt.Errorf("while reading source info: %s", err)
	}

	sourceWriter, err := sourceInfo.NewWriter(output)
	if err != nil {
		return fmt.Errorf("while making source writer: %s", err)
	}
	multiWriter := io.MultiWriter(sourceWriter, shakeHash)

	targetReader := targetInfo.NewReader(target)

	rs := mkrsync()

	ops := make(chan rsync.Operation)
	errc := make(chan error, 1)

	go readOps(brc, ops, errc)

	err = rs.ApplyRecipe(multiWriter, targetReader, ops)
	if err != nil {
		return fmt.Errorf("While applying recipe: %s", err.Error())
	}

	err = <-errc
	if err != nil {
		return fmt.Errorf("While reading recipe: %s", err.Error())
	}

	var b bytes.Buffer
	_, err = io.CopyN(&b, shakeHash, 32)
	if err != nil {
		return err
	}

	computedHash := b.Bytes()

	tlcHash := &Hash{}
	err = brc.ReadMessage(tlcHash)
	if err != nil {
		return err
	}

	if tlcHash.Type != header.FullHashType {
		return ErrMalformedRecipe
	}
	expectedHash := tlcHash.Contents

	if !bytes.Equal(computedHash, expectedHash) {
		return fmt.Errorf("integrity checked failed: has %x, expected %x", computedHash, expectedHash)
	}

	return nil
}

func readOps(rc *wire.ReadContext, ops chan rsync.Operation, errc chan error) {
	defer close(ops)
	totalOps := 0
	opsCount := []int{0, 0, 0}
	opsBytes := []int64{0, 0, 0}

	rop := &RsyncOp{}
	op := rsync.Operation{}

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
		case RsyncOp_BLOCK:
			op.Type = rsync.OpBlock
			op.BlockIndex = rop.BlockIndex
			opsBytes[op.Type] += int64(BlockSize)

		case RsyncOp_BLOCK_RANGE:
			op.Type = rsync.OpBlockRange
			op.BlockIndex = rop.BlockIndex
			op.BlockIndexEnd = rop.BlockIndexEnd
			opsBytes[op.Type] += int64(BlockSize) * int64(op.BlockIndexEnd-op.BlockIndex)

		case RsyncOp_DATA:
			op.Type = rsync.OpData
			op.Data = rop.Data
			opsBytes[op.Type] += int64(len(op.Data))

		default:
			hasOp = false
			switch rop.Type {
			case RsyncOp_HEY_YOU_DID_IT:
				readingOps = false
			default:
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

	errc <- nil
}

func readRepoInfo(rc *wire.ReadContext) (*tlc.RepoInfo, error) {
	pi := &RepoInfo{}
	err := rc.ReadMessage(pi)
	if err != nil {
		return nil, err
	}

	ri := &tlc.RepoInfo{
		BlockSize: BlockSize,
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

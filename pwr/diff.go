package pwr

import (
	"bytes"
	"fmt"
	"io"

	"golang.org/x/crypto/sha3"

	"gopkg.in/kothar/brotli-go.v0/enc"

	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/rsync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

type ProgressCallback func(percent float64)

func ComputeSignature(path string, info *tlc.RepoInfo, onProgress ProgressCallback) (signature []rsync.BlockHash, err error) {
	r := info.NewReader(path)
	defer r.Close()

	rs := mkrsync()
	signature = make([]rsync.BlockHash, 0, info.NumBlocks)

	paddedBytes := info.NumBlocks * int64(BLOCK_SIZE)

	onRead := func(count int64) {
		onProgress(100.0 * float64(count) / float64(paddedBytes))
	}
	cr := counter.NewReaderCallback(onRead, r)

	sigWriter := func(bl rsync.BlockHash) error {
		signature = append(signature, bl)
		return nil
	}
	err = rs.CreateSignature(cr, sigWriter)
	if err != nil {
		return nil, err
	}

	return signature, nil
}

func WriteRecipe(
	patchWriter io.Writer,
	sourceInfo *tlc.RepoInfo, sourceReader io.Reader,
	targetInfo *tlc.RepoInfo, signature []rsync.BlockHash,
	onProgress ProgressCallback,
	brotliParams *enc.BrotliParams) error {

	shakeHash := sha3.NewShake256()
	teeReader := io.TeeReader(sourceReader, shakeHash)

	wc := wire.NewWriteContext(patchWriter)

	header := &RecipeHeader{}
	header.Version = RecipeHeader_V1
	header.Compression = RecipeHeader_BROTLI
	header.CompressionLevel = 1

	err := wc.WriteMessage(header)
	if err != nil {
		return err
	}

	bw := enc.NewBrotliWriter(brotliParams, patchWriter)
	bwc := wire.NewWriteContext(bw)

	writeRepoInfo(bwc, targetInfo)
	writeRepoInfo(bwc, sourceInfo)

	sourcePaddedBytes := sourceInfo.NumBlocks * int64(BLOCK_SIZE)
	onSourceRead := func(count int64) {
		onProgress(100.0 * float64(count) / float64(sourcePaddedBytes))
	}
	sourceReaderCounter := counter.NewReaderCallback(onSourceRead, teeReader)

	numOps := 0
	wop := &RsyncOp{}

	opsWriter := func(op rsync.Operation) error {
		numOps++
		wop.Reset()

		switch op.Type {
		case rsync.OpBlock:
			wop.Type = RsyncOp_BLOCK
			wop.BlockIndex = op.BlockIndex
		case rsync.OpBlockRange:
			wop.Type = RsyncOp_BLOCK_RANGE
			wop.BlockIndex = op.BlockIndex
			wop.BlockIndexEnd = op.BlockIndexEnd
		case rsync.OpData:
			wop.Type = RsyncOp_DATA
			wop.Data = op.Data
		default:
			return fmt.Errorf("unknown rsync op type: %d", op.Type)
		}

		err := bwc.WriteMessage(wop)
		if err != nil {
			return err
		}

		return nil
	}

	rs := mkrsync()
	err = rs.InventRecipe(sourceReaderCounter, signature, opsWriter)
	if err != nil {
		return err
	}

	eop := &RsyncOp{}
	eop.Type = RsyncOp_HEY_YOU_DID_IT
	err = bwc.WriteMessage(eop)
	if err != nil {
		return err
	}

	var b bytes.Buffer
	io.CopyN(&b, shakeHash, 64)

	tlcHash := &Hash{}
	tlcHash.Type = HashType_SHAKESUM256
	tlcHash.Contents = b.Bytes()

	err = bwc.WriteMessage(tlcHash)
	if err != nil {
		return err
	}

	err = bw.Close()
	if err != nil {
		return err
	}

	return nil
}

func writeRepoInfo(bwc *wire.WriteContext, info *tlc.RepoInfo) error {
	dirs := make([]*RepoInfo_Dir, 0, len(info.Dirs))
	for _, d := range info.Dirs {
		dirs = append(dirs, &RepoInfo_Dir{
			Path: d.Path,
			Mode: uint32(d.Mode),
		})
	}

	files := make([]*RepoInfo_File, 0, len(info.Files))
	for _, f := range info.Files {
		files = append(files, &RepoInfo_File{
			Path:          f.Path,
			Mode:          uint32(f.Mode),
			Size:          f.Size,
			BlockIndex:    f.BlockIndex,
			BlockIndexEnd: f.BlockIndexEnd,
		})
	}

	symlinks := make([]*RepoInfo_Symlink, 0, len(info.Symlinks))
	for _, s := range info.Symlinks {
		symlinks = append(symlinks, &RepoInfo_Symlink{
			Path: s.Path,
			Mode: uint32(s.Mode),
			Dest: s.Dest,
		})
	}

	ri := &RepoInfo{
		NumBlocks: info.NumBlocks,
		Dirs:      dirs,
		Files:     files,
		Symlinks:  symlinks,
	}

	return bwc.WriteMessage(ri)
}

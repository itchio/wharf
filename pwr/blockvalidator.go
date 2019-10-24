package pwr

import (
	"bytes"

	"github.com/itchio/wharf/wsync"
	"github.com/pkg/errors"
)

type blockValidator struct {
	hashInfo *HashInfo
	sctx     *wsync.Context
}

type BlockValidator interface {
	BlockSize(fileIndex int64, blockIndex int64) int64
	ValidateAsError(fileIndex int64, blockIndex int64, data []byte) error
	ValidateAsWound(fileIndex int64, blockIndex int64, data []byte) Wound
}

func NewBlockValidator(hashInfo *HashInfo) BlockValidator {
	return &blockValidator{
		hashInfo: hashInfo,
		sctx:     wsync.NewContext(int(BlockSize)),
	}
}

func (bv *blockValidator) BlockSize(fileIndex int64, blockIndex int64) int64 {
	fileSize := bv.hashInfo.Container.Files[fileIndex].Size
	return ComputeBlockSize(fileSize, blockIndex)
}

func (bv *blockValidator) ValidateAsWound(fileIndex int64, blockIndex int64, data []byte) Wound {
	weakHash, strongHash := bv.sctx.HashBlock(data)
	hashGroup := bv.hashInfo.Groups[fileIndex]
	start := blockIndex * BlockSize
	size := bv.BlockSize(fileIndex, blockIndex)

	if blockIndex >= int64(len(hashGroup)) {
		return Wound{
			Kind:  WoundKind_FILE,
			Index: fileIndex,
			Start: start,
			End:   start + size,
		}
	}

	bh := hashGroup[blockIndex]

	if bh.WeakHash != weakHash {
		return Wound{
			Kind:  WoundKind_FILE,
			Index: fileIndex,
			Start: start,
			End:   start + size,
		}
	}

	if !bytes.Equal(bh.StrongHash, strongHash) {
		return Wound{
			Kind:  WoundKind_FILE,
			Index: fileIndex,
			Start: start,
			End:   start + size,
		}
	}

	return Wound{
		Kind:  WoundKind_CLOSED_FILE,
		Index: fileIndex,
		Start: start,
		End:   start + size,
	}
}

func (bv *blockValidator) ValidateAsError(fileIndex int64, blockIndex int64, data []byte) error {
	weakHash, strongHash := bv.sctx.HashBlock(data)
	hashGroup := bv.hashInfo.Groups[fileIndex]
	file := bv.hashInfo.Container.Files[fileIndex]

	if blockIndex >= int64(len(hashGroup)) {
		return errors.Errorf("%s: too large (%d blocks, tried to look up hash %d)",
			file.Path, len(hashGroup), blockIndex)
	}

	bh := hashGroup[blockIndex]

	if bh.WeakHash != weakHash {
		return errors.Errorf("(%s) at block %d, expected weak hash %x, got %x", file.Path, blockIndex, bh.WeakHash, weakHash)
	}

	if !bytes.Equal(bh.StrongHash, strongHash) {
		return errors.Errorf("(%s) at block %d, expected strong hash %x, got %x", file.Path, blockIndex, bh.StrongHash, strongHash)
	}

	return nil
}

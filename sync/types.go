package sync

import (
	"hash"
	"io"
)

// Internal constant used in rolling checksum.
const _M = 1 << 16

// Operation Types.
type OpType byte

const (
	OpBlockRange OpType = iota
	OpData
)

// Instruction to mutate target to align to source.
type Operation struct {
	Type       OpType
	FileIndex  int64
	BlockIndex int64
	BlockSpan  int64
	Data       []byte
}

type OperationWriter func(op Operation) error

// Signature hash item generated from target.
type BlockHash struct {
	FileIndex  int64
	BlockIndex int64
	WeakHash   uint32
	StrongHash []byte

	// ShortSize specifies the block size when non-zero
	ShortSize int32
}

type SignatureWriter func(hash BlockHash) error

type SyncContext struct {
	blockSize    int
	buffer       []byte
	uniqueHasher hash.Hash
}

type FilePool interface {
	GetReader(fileIndex int64) (io.ReadSeeker, error)
	Close() error
}

type BlockLibrary struct {
	hashLookup map[uint32][]BlockHash
}

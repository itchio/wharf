package pwr

import (
	"encoding/binary"

	"github.com/itchio/wharf/sync"
)

var endianness = binary.LittleEndian

const (
	pwrMagic = int32(iota + 0xFEF5F00)
)

// BlockSize is a compromise between wasted hashing work (because of padding)
// and inefficient diffs
// var BlockSize = 4 * 1024 // 16k
var BlockSize = 4 // 4 bytes for test

func mksync() *sync.SyncContext {
	return sync.NewContext(BlockSize)
}

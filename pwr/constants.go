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
var BlockSize = 16 * 1024 // 16k

func mksync() *sync.SyncContext {
	return sync.NewContext(BlockSize)
}

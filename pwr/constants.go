package pwr

import (
	"encoding/binary"

	"github.com/itchio/wharf/sync"
)

var endianness = binary.LittleEndian

const (
	recipeMagic = int32(iota + 0xFEF5F00)
	signatureMagic
)

var BlockSize = 64 * 1024 // 64k

func mksync() *sync.SyncContext {
	return sync.NewContext(BlockSize)
}

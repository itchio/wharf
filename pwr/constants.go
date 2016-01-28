package pwr

import "encoding/binary"

var endianness = binary.LittleEndian

const (
	pwrMagic = int32(iota + 0xFEF5F00)
)

// BlockSize is a compromise between wasted hashing work (because of padding)
// and inefficient diffs
var BlockSize = 8 * 1024

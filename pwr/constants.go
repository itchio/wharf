package pwr

import "encoding/binary"

var ENDIANNESS = binary.LittleEndian // gotta pick one
var BLOCK_SIZE = 16 * 1024           // use 16KB blocks

const (
	PWR_MAGIC = int32(iota + 0xFEF5F00)
	PWR_BROTLI
)

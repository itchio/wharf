package pwr

import "encoding/binary"

var ENDIANNESS = binary.LittleEndian // gotta pick one
var BLOCK_SIZE = 4 * 1024

const (
	PWR_MAGIC = int32(iota + 0xFEF5F00)
	PWR_BROTLI
)

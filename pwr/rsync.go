package pwr

import (
	"crypto/md5"

	"github.com/itchio/wharf/rsync"
)

func mkrsync() *rsync.RSync {
	return &rsync.RSync{
		BlockSize:    BLOCK_SIZE,
		UniqueHasher: md5.New(),
	}
}

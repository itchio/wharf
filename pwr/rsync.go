package pwr

import (
	"crypto/md5"

	"github.com/itchio/wharf/rsync"
)

func mkrsync() *rsync.RSync {
	return &rsync.RSync{
		BlockSize:    BlockSize,
		UniqueHasher: md5.New(),
	}
}

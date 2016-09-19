package tlc

import (
	"archive/zip"
	"io"
	"os"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/sync"
)

// ReadCloseSeeker unifies io.Reader, io.Seeker, and io.Closer
type ReadCloseSeeker interface {
	io.Reader
	io.Seeker
	io.Closer
}

func (c *Container) NewPool(basePath string) (sync.FilePool, error) {
	if basePath == "/dev/null" {
		return c.NewFilePool(basePath), nil
	}

	targetInfo, err := os.Lstat(basePath)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	if targetInfo.IsDir() {
		return c.NewFilePool(basePath), nil
	} else {
		fr, err := os.Open(basePath)
		if err != nil {
			return nil, errors.Wrap(err, 1)
		}

		zr, err := zip.NewReader(fr, targetInfo.Size())
		if err != nil {
			return nil, errors.Wrap(err, 1)
		}

		return c.NewZipPool(zr), nil
	}
}

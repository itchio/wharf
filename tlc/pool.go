package tlc

import (
	"io"
	"os"
	"path/filepath"

	"github.com/itchio/wharf/sync"
)

type ReadCloseSeeker interface {
	io.Reader
	io.Seeker
	io.Closer
}

type ContainerFilePool struct {
	container *Container
	basePath  string

	fileIndex int64
	reader    ReadCloseSeeker
}

var _ sync.FilePool = (*ContainerFilePool)(nil)

func (c *Container) NewFilePool(basePath string) *ContainerFilePool {
	return &ContainerFilePool{
		container: c,
		basePath:  basePath,

		fileIndex: int64(-1),
		reader:    nil,
	}
}

func (cfp *ContainerFilePool) GetPath(fileIndex int64) string {
	path := filepath.FromSlash(cfp.container.Files[fileIndex].Path)
	fullPath := filepath.Join(cfp.basePath, path)
	return fullPath
}

func (cfp *ContainerFilePool) GetReader(fileIndex int64) (io.ReadSeeker, error) {
	if cfp.fileIndex != fileIndex {
		if cfp.reader != nil {
			cfp.reader.Close()
		}

		reader, err := os.Open(cfp.GetPath(fileIndex))
		if err != nil {
			return nil, err
		}

		cfp.reader = reader
	}

	return cfp.reader, nil
}

func (cfp *ContainerFilePool) Close() error {
	if cfp.reader != nil {
		err := cfp.reader.Close()
		if err != nil {
			return err
		}

		cfp.reader = nil
	}

	return nil
}

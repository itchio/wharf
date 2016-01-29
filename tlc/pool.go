package tlc

import (
	"io"
	"os"

	"github.com/itchio/wharf/sync"
)

type ReadCloseSeeker interface {
	io.Reader
	io.Seeker
	io.Closer
}

type ContainerFilePool struct {
	container *Container

	fileIndex int64
	reader    ReadCloseSeeker
}

func (c *Container) NewFilePool() sync.FilePool {
	return &ContainerFilePool{
		container: c,

		fileIndex: int64(-1),
		reader:    nil,
	}
}

func (cfp *ContainerFilePool) GetReader(fileIndex int64) (io.ReadSeeker, error) {
	if cfp.fileIndex != fileIndex {
		if cfp.reader != nil {
			cfp.reader.Close()
		}

		reader, err := os.Open(cfp.container.Files[fileIndex].Path)
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

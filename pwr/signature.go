package pwr

import (
	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/sync"
	"github.com/itchio/wharf/tlc"
)

// ComputeDiffSignature returns a series of hash suitable to create a diff
func ComputeDiffSignature(container *tlc.Container, basePath string, consumer *StateConsumer) (signature []sync.BlockHash, err error) {
	pool := container.NewFilePool(basePath)
	defer pool.Close()

	sctx := mksync()
	signature = make([]sync.BlockHash, 0)

	totalBytes := container.Size
	fileOffset := int64(0)

	onRead := func(count int64) {
		consumer.Progress(100.0 * float64(fileOffset+count) / float64(totalBytes))
	}

	sigWriter := func(bl sync.BlockHash) error {
		signature = append(signature, bl)
		return nil
	}

	for fileIndex, f := range container.Files {
		fileOffset = f.Offset

		reader, err := pool.GetReader(int64(fileIndex))
		if err != nil {
			return nil, err
		}

		cr := counter.NewReaderCallback(onRead, reader)
		err = sctx.CreateSignature(int64(fileIndex), cr, sigWriter)
		if err != nil {
			return nil, err
		}
	}

	return signature, nil
}

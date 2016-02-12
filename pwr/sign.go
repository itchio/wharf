package pwr

import (
	"io"

	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/sync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

func ComputeSignature(container *tlc.Container, basePath string, consumer *StateConsumer) ([]sync.BlockHash, error) {
	signature := make([]sync.BlockHash, 0)

	err := ComputeSignatureToWriter(container, basePath, consumer, func(bl sync.BlockHash) error {
		signature = append(signature, bl)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return signature, nil
}

// ComputeSignature returns a series of hash suitable to create a diff or to verify the integrity of a file
func ComputeSignatureToWriter(container *tlc.Container, basePath string, consumer *StateConsumer, sigWriter sync.SignatureWriter) error {
	pool := container.NewFilePool(basePath)
	defer pool.Close()

	sctx := mksync()

	totalBytes := container.Size
	fileOffset := int64(0)

	onRead := func(count int64) {
		consumer.Progress(100.0 * float64(fileOffset+count) / float64(totalBytes))
	}

	for fileIndex, f := range container.Files {
		consumer.ProgressLabel(f.Path)
		fileOffset = f.Offset

		reader, err := pool.GetReader(int64(fileIndex))
		if err != nil {
			return err
		}

		cr := counter.NewReaderCallback(onRead, reader)
		err = sctx.CreateSignature(int64(fileIndex), cr, sigWriter)
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadSignature(signatureReader io.Reader) (*tlc.Container, []sync.BlockHash, error) {
	rawSigWire := wire.NewReadContext(signatureReader)
	err := rawSigWire.ExpectMagic(SignatureMagic)
	if err != nil {
		return nil, nil, err
	}

	header := &SignatureHeader{}
	err = rawSigWire.ReadMessage(header)
	if err != nil {
		return nil, nil, err
	}

	sigWire, err := UncompressWire(rawSigWire, header.Compression)
	if err != nil {
		return nil, nil, err
	}

	container := &tlc.Container{}
	err = sigWire.ReadMessage(container)
	if err != nil {
		return nil, nil, err
	}

	signature := make([]sync.BlockHash, 0)
	hash := &BlockHash{}

	for {
		hash.Reset()
		err = sigWire.ReadMessage(hash)

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}

		signature = append(signature, sync.BlockHash{
			WeakHash:   hash.WeakHash,
			StrongHash: hash.StrongHash,
		})
	}

	return container, signature, nil
}

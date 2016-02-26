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
		consumer.Progress(float64(fileOffset+count) / float64(totalBytes))
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

	blockIndex := int64(0)
	fileIndex := int64(0)
	byteOffset := int64(0)
	blockSize64 := int64(BlockSize)

	for {
		hash.Reset()
		err = sigWire.ReadMessage(hash)

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}

		sizeDiff := container.Files[fileIndex].Size - byteOffset
		shortSize := int32(0)

		if sizeDiff < 0 {
			byteOffset = 0
			blockIndex = 0
			fileIndex++
			sizeDiff = container.Files[fileIndex].Size - byteOffset
		}

		// last block
		if sizeDiff < blockSize64 {
			shortSize = int32(sizeDiff)
		} else {
			shortSize = 0
		}

		signature = append(signature, sync.BlockHash{
			FileIndex:  fileIndex,
			BlockIndex: blockIndex,

			WeakHash:   hash.WeakHash,
			StrongHash: hash.StrongHash,

			ShortSize: shortSize,
		})

		// still in same file
		byteOffset += blockSize64
		blockIndex++
	}

	return container, signature, nil
}

package pwr

import (
	"io"

	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/sync"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
)

// ComputeSignature compute the signature of all blocks of all files in a given container,
// by reading them from disk, relative to `basePath`, and notifying `consumer` of its
// progress
func ComputeSignature(container *tlc.Container, basePath string, consumer *StateConsumer) ([]sync.BlockHash, error) {
	var signature []sync.BlockHash

	err := ComputeSignatureToWriter(container, basePath, consumer, func(bl sync.BlockHash) error {
		signature = append(signature, bl)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return signature, nil
}

// ComputeSignatureToWriter is a variant of ComputeSignature that writes hashes
// to a callback
func ComputeSignatureToWriter(container *tlc.Container, basePath string, consumer *StateConsumer, sigWriter sync.SignatureWriter) error {
	var err error

	pool := container.NewFilePool(basePath)
	defer func() {
		if pErr := pool.Close(); pErr != nil && err == nil {
			err = pErr
		}
	}()

	sctx := mksync()

	totalBytes := container.Size
	fileOffset := int64(0)

	onRead := func(count int64) {
		consumer.Progress(float64(fileOffset+count) / float64(totalBytes))
	}

	for fileIndex, f := range container.Files {
		consumer.ProgressLabel(f.Path)
		fileOffset = f.Offset

		var reader io.ReadSeeker
		reader, err = pool.GetReader(int64(fileIndex))
		if err != nil {
			return err
		}

		cr := counter.NewReaderCallback(onRead, reader)
		err = sctx.CreateSignature(int64(fileIndex), cr, sigWriter)
		if err != nil {
			return err
		}
	}

	return err
}

// ReadSignature reads the hashes from all files of a given container, from a
// wharf signature file.
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

	var signature []sync.BlockHash
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

package pwr

import (
	"github.com/itchio/wharf/wsync"
	"github.com/pkg/errors"
)

type HashGroups = map[int64][]wsync.BlockHash

func MakeHashGroups(sigInfo *SignatureInfo) (HashGroups, error) {
	pathToFileIndex := make(map[string]int64)
	for fileIndex, f := range sigInfo.Container.Files {
		pathToFileIndex[f.Path] = int64(fileIndex)
	}

	hashGroups := make(HashGroups)
	hashIndex := int64(0)

	for _, f := range sigInfo.Container.Files {
		fileIndex := pathToFileIndex[f.Path]

		if f.Size == 0 {
			// empty files have a 0-length shortblock for historical reasons.
			hashIndex++
			continue
		}

		numBlocks := ComputeNumBlocks(f.Size)
		hashGroups[fileIndex] = sigInfo.Hashes[hashIndex : hashIndex+numBlocks]
		hashIndex += numBlocks
	}

	if hashIndex != int64(len(sigInfo.Hashes)) {
		err := errors.Errorf("expected to have %d hashes in signature, had %d", hashIndex, len(vp.Signature.Hashes))
		return nil, errors.WithStack(err)
	}

	return hashGroups, nil
}

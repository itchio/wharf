package sync

import "fmt"

func NewBlockLibrary(hashes []BlockHash) *BlockLibrary {
	fmt.Printf("Creating new library with %d hashes\n", len(hashes))

	// A single β-hash may correlate with many unique hashes.
	hashLookup := make(map[uint32][]BlockHash)

	for _, hash := range hashes {
		key := hash.WeakHash
		if hashLookup[key] == nil {
			hashLookup[key] = []BlockHash{hash}
		} else {
			hashLookup[key] = append(hashLookup[key], hash)
		}
	}

	return &BlockLibrary{hashLookup}
}

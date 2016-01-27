package rsync_test

import (
	"os"

	"github.com/itchio/wharf/rsync"
)

func Example() {
	srcReader, _ := os.Open("content-v2.bin")
	defer srcReader.Close()

	rs := &rsync.RSync{}

	// here we store the whole signature in a byte slice,
	// but it could just as well be sent over a network connection for example
	sig := make([]rsync.BlockHash, 0, 10)
	writeSignature := func(bl rsync.BlockHash) error {
		sig = append(sig, bl)
		return nil
	}

	rs.CreateSignature(srcReader, writeSignature)

	targetReader, _ := os.Open("content-v1.bin")

	opsOut := make(chan rsync.Operation)
	writeOperation := func(op rsync.Operation) error {
		opsOut <- op
		return nil
	}

	go func() {
		defer close(opsOut)
		rs.InventRecipe(targetReader, sig, writeOperation)
	}()

	srcWriter, _ := os.Open("content-v2-reconstructed.bin")
	srcReader.Seek(0, os.SEEK_SET)

	rs.ApplyRecipe(srcWriter, srcReader, opsOut)
}

package tlc_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/wharf/tlc"
	"github.com/stretchr/testify/assert"
)

func Test_Write(t *testing.T) {
	tmpPath := mktestdir(t, "write_read")
	defer os.RemoveAll(tmpPath)

	t.Logf("walking sample dir")
	info, err := tlc.Walk(tmpPath, 16)
	must(t, err)

	t.Logf("creates writer with repo info")
	wmpPath, err := ioutil.TempDir(".", "tmp_write_write")
	must(t, err)
	defer os.RemoveAll(wmpPath)

	w, err := info.NewWriter(wmpPath)
	must(t, err)

	t.Logf("comparing directory structure")
	info2, err := tlc.Walk(wmpPath, 16)
	must(t, err)
	assert.Equal(t, info, info2, "creates same directory structure")

	t.Logf("writing fefs through whole writer")
	fefs := make([]byte, 0)
	for i := int64(0); i < info.NumBlocks; i++ {
		fefs = appendFiller(fefs, 0xf, info.BlockSize)
	}

	written, err := w.Write(fefs)
	t.Logf("written %d, err %s", written, err)
	must(t, err)

	t.Logf("making sure all files are full of fefs")
	for _, file := range info.Files {
		fullpath := filepath.Join(wmpPath, file.Path)
		assertFefs(t, fullpath)
	}

	must(t, w.Close())
}

func assertFefs(t *testing.T, fullpath string) {
	contents, err := ioutil.ReadFile(fullpath)
	must(t, err)

	fefs := appendFiller(make([]byte, 0), 0xf, len(contents))
	assert.Equal(t, contents, fefs, "should be fefs")
}

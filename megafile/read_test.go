package megafile_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/itchio/wharf.proto/megafile"
	"github.com/stretchr/testify/assert"
)

func Test_Read(t *testing.T) {
	tmpPath := mktestdir(t, "read")
	defer os.RemoveAll(tmpPath)

	t.Logf("walking sample dir")
	info, err := megafile.Walk(tmpPath, 16)
	must(t, err)

	r := info.NewReader(tmpPath)

	t.Logf("reading whole megafile")
	all, err := ioutil.ReadAll(r)
	must(t, err)

	t.Logf("testing megafile layout")
	assert.Equal(t, len(all), info.BlockSize*int(info.NumBlocks), "has right length")

	expected := make([]byte, 0, 0)
	expected = paddedAppend(expected, 0xa, 10)
	expected = paddedAppend(expected, 0xb, 20)
	expected = paddedAppend(expected, 0xc, 30)
	expected = paddedAppend(expected, 0xd, 50)
	expected = paddedAppend(expected, 0xe, 40)

	assert.Equal(t, all, expected, "has padded file layout")

	t.Logf("testing megafile random access")
	single := make([]byte, 1)

	offset, err := r.Seek(0, os.SEEK_SET)
	must(t, err)
	assert.Equal(t, offset, int64(0), "seeks to beginning")

	read, err := r.Read(single)
	must(t, err)
	assert.Equal(t, read, 1, "read 1 byte")
	assert.Equal(t, single[0], byte(0xa), "reads from first file")

	offset, err = r.Seek(20, os.SEEK_SET)
	must(t, err)
	assert.Equal(t, offset, int64(20), "seeks inside second file")

	read, err = r.Read(single)
	must(t, err)
	assert.Equal(t, read, 1, "read 1 byte")
	assert.Equal(t, single[0], byte(0xb), "reads from second file")

	offset, err = r.Seek(11, os.SEEK_SET)
	must(t, err)
	assert.Equal(t, offset, int64(11), "seeks inside first file padding")

	multi := make([]byte, 512)
	expectedMulti := appendFiller(make([]byte, 0), 0x0, 16-11)

	read, err = r.Read(multi)
	must(t, err)
	assert.Equal(t, read, 16-11, "read all available padding")
	assert.Equal(t, multi[:read], expectedMulti, "reads from second file")

	expectedMulti = appendFiller(make([]byte, 0), 0xb, 20)
	read, err = r.Read(multi)
	must(t, err)
	assert.Equal(t, read, 20, "reads entire second file (across two blocks)")
	assert.Equal(t, multi[:read], expectedMulti, "reads from second file")

	expectedMulti = appendFiller(make([]byte, 0), 0x0, 12)
	read, err = r.Read(multi)
	must(t, err)
	assert.Equal(t, read, 12, "reads entire second file padding")
	assert.Equal(t, multi[:read], expectedMulti, "reads from second file padding")

	t.Logf("closing megafile")
	must(t, r.Close())
}

func paddedAppend(slice []byte, b byte, q int) []byte {
	slice = appendFiller(slice, b, q)
	slice = appendFiller(slice, 0, 16-q%16)
	return slice
}

func appendFiller(slice []byte, b byte, q int) []byte {
	for i := 0; i < q; i++ {
		slice = append(slice, b)
	}
	return slice
}

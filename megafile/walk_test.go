package megafile_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/wharf.proto/megafile"
	"github.com/stretchr/testify/assert"
)

type regEntry struct {
	Path string
	Size int
}

type symlinkEntry struct {
	Oldname string
	Newname string
}

func must(t *testing.T, err error) {
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
}

func Test_Walk(t *testing.T) {
	tmpPath, err := ioutil.TempDir("tmp", "megafile_walk")
	must(t, err)

	regulars := []regEntry{
		{"foo/file_f", 124},
		{"foo/dir_a/baz", 623},
		{"foo/dir_b/zoom", 623},
		{"foo/file_z", 371},
		{"foo/dir_a/bazzz", 6230},
	}
	symlinks := []symlinkEntry{
		{"file_z", "foo/file_m"},
		{"dir_a/baz", "foo/file_o"},
	}

	filler := []byte{42}

	must(t, os.RemoveAll(tmpPath))

	for _, entry := range regulars {
		fullPath := filepath.Join(tmpPath, entry.Path)
		must(t, os.MkdirAll(filepath.Dir(fullPath), os.FileMode(0777)))
		file, err := os.Create(fullPath)
		must(t, err)

		for i := 0; i < entry.Size; i++ {
			_, err := file.Write(filler)
			must(t, err)
		}
		must(t, file.Close())
	}

	for _, entry := range symlinks {
		new := filepath.Join(tmpPath, entry.Newname)
		must(t, os.Symlink(entry.Oldname, new))
	}

	info, err := megafile.Walk(tmpPath, 16)
	must(t, err)

	dirs := []string{
		".",
		"foo",
		"foo/dir_a",
		"foo/dir_b",
	}
	for i, dir := range dirs {
		assert.Equal(t, dir, info.Dirs[i].Path, "dirs should be all listed")
	}

	files := []string{
		"foo/dir_a/baz",
		"foo/dir_a/bazzz",
		"foo/dir_b/zoom",
		"foo/file_f",
		"foo/file_z",
	}
	for i, file := range files {
		assert.Equal(t, file, info.Files[i].Path, "files should be all listed")
	}

	for i, symlink := range symlinks {
		assert.Equal(t, symlink.Newname, info.Symlinks[i].Path, "symlink should be at correct path")
		assert.Equal(t, symlink.Oldname, info.Symlinks[i].Dest, "symlink should point to correct path")
	}

	must(t, os.RemoveAll(tmpPath))
}

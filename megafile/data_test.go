package megafile_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func must(t *testing.T, err error) {
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
}

type regEntry struct {
	Path string
	Size int
}

type symlinkEntry struct {
	Oldname string
	Newname string
}

var regulars = []regEntry{
	{"foo/file_f", 124},
	{"foo/dir_a/baz", 623},
	{"foo/dir_b/zoom", 421},
	{"foo/file_z", 371},
	{"foo/dir_a/bazzz", 6230},
}

var symlinks = []symlinkEntry{
	{"file_z", "foo/file_m"},
	{"dir_a/baz", "foo/file_o"},
}

var filler = []byte{42}

func mktestdir(t *testing.T) string {
	tmpPath, err := ioutil.TempDir("tmp", "megafile_walk")
	must(t, err)

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

	return tmpPath
}

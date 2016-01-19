package megafile_test

import (
	"os"
	"testing"

	"github.com/itchio/wharf.proto/megafile"
	"github.com/stretchr/testify/assert"
)

func Test_Walk(t *testing.T) {
	tmpPath := mktestdir(t, "walk")
	defer os.RemoveAll(tmpPath)

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
}

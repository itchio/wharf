package megafile

import (
	"errors"
	"io"
)

type Writer struct {
	basePath string
	info     *RepoInfo
}

func (info *RepoInfo) NewWriter(basePath string) *Writer {
	// XXX create all directories
	// XXX create all symlinks
	// XXX create all the files with the right permissions
	// XXX gate writes by juggling actual file writers,
	// ignoring padding, etc.
	return &Writer{basePath, info}
}

func (w *Writer) Write(p []byte) (int, error) {
	return 0, errors.New("stub")
}

func (w *Writer) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("stub")
}

var _ io.WriteSeeker = (*Writer)(nil)

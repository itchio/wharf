package megafile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Writer struct {
	basePath string
	info     *RepoInfo
}

func (info *RepoInfo) NewWriter(basePath string) (*Writer, error) {
	for _, dirEntry := range info.Dirs {
		fullPath := filepath.Join(basePath, dirEntry.Path)
		fmt.Printf("mkdir -p %s %d\n", fullPath, dirEntry.Mode)
		err := os.MkdirAll(fullPath, dirEntry.Mode)
		if err != nil {
			return nil, err
		}
		err = os.Chmod(fullPath, dirEntry.Mode)
		if err != nil {
			return nil, err
		}
	}

	for _, fileEntry := range info.Files {
		fullPath := filepath.Join(basePath, fileEntry.Path)
		fmt.Printf("touch %s %d\n", fullPath, fileEntry.Mode)
		file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC, fileEntry.Mode)
		if err != nil {
			return nil, err
		}
		err = file.Close()
		if err != nil {
			return nil, err
		}

		err = os.Chmod(fullPath, fileEntry.Mode)
		if err != nil {
			return nil, err
		}

		err = os.Truncate(fullPath, fileEntry.Size)
		if err != nil {
			return nil, err
		}
	}

	for _, link := range info.Symlinks {
		fullPath := filepath.Join(basePath, link.Path)
		fmt.Printf("ln -s %s %s\n", link.Dest, fullPath)
		err := os.Symlink(link.Dest, fullPath)
		if err != nil {
			return nil, err
		}
	}

	return &Writer{basePath, info}, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	return 0, errors.New("stub")
}

func (w *Writer) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("stub")
}

func (w *Writer) Close() error {
	return nil
}

var _ io.WriteSeeker = (*Writer)(nil)
var _ io.Closer = (*Writer)(nil)

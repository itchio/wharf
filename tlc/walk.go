package tlc

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	ModeMask = 0644
)

type FilterFunc func(fileInfo os.FileInfo) bool

func Walk(BasePath string, filter FilterFunc) (*Container, error) {
	if filter == nil {
		// default filter is a passthrough
		filter = func(fileInfo os.FileInfo) bool {
			return true
		}
	}

	Dirs := make([]*Dir, 0, 0)
	Symlinks := make([]*Symlink, 0, 0)
	Files := make([]*File, 0, 0)

	TotalOffset := int64(0)

	onEntry := func(FullPath string, fileInfo os.FileInfo, err error) error {
		// we shouldn't encounter any error crawling the repo
		if err != nil {
			if os.IsPermission(err) {
				// ...except permission errors, those are fine
			} else {
				return err
			}
		}

		Path, err := filepath.Rel(BasePath, FullPath)
		if err != nil {
			return err
		}

		Path = filepath.ToSlash(Path)

		// don't end up with files we (the patcher) can't modify
		Mode := fileInfo.Mode() | ModeMask

		if !filter(fileInfo) {
			if Mode.IsDir() {
				return filepath.SkipDir
			} else {
				return nil
			}
		}

		if Mode.IsDir() {
			Dirs = append(Dirs, &Dir{Path: Path, Mode: uint32(Mode)})
		} else if Mode.IsRegular() {
			Size := fileInfo.Size()
			Offset := TotalOffset
			OffsetEnd := Offset + Size

			Files = append(Files, &File{Path: Path, Mode: uint32(Mode), Size: Size, Offset: Offset})
			TotalOffset = OffsetEnd
		} else if Mode&os.ModeSymlink > 0 {
			Dest, err := os.Readlink(FullPath)
			if err != nil {
				return err
			}

			Dest = filepath.ToSlash(Dest)
			Symlinks = append(Symlinks, &Symlink{Path: Path, Mode: uint32(Mode), Dest: Dest})
		}

		return nil
	}

	if BasePath == "/dev/null" {
		// empty container is fine - /dev/null is legal even on Win32 where it doesn't exist
	} else {
		fi, err := os.Lstat(BasePath)
		if err != nil {
			return nil, err
		}

		if !fi.IsDir() {
			return nil, fmt.Errorf("tlc: can't walk non-directory %s", BasePath)
		}

		err = filepath.Walk(BasePath, onEntry)
		if err != nil {
			return nil, err
		}
	}

	container := &Container{Size: TotalOffset, Dirs: Dirs, Symlinks: Symlinks, Files: Files}
	return container, nil
}

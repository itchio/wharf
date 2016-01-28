package tlc

import (
	"os"
	"path/filepath"
)

const (
	MODE_MASK = 0644
)

func Walk(BasePath string, BlockSize int) (*RepoInfo, error) {
	Dirs := make([]Dir, 0, 0)
	Symlinks := make([]Symlink, 0, 0)
	Files := make([]File, 0, 0)

	BlockIndex := int64(0)

	onEntry := func(FullPath string, fi os.FileInfo, err error) error {
		// we shouldn't encounter any error crawling the repo
		if err != nil {
			if os.IsPermission(err) {
				// ignore
			} else {
				return err
			}
		}

		Path, err := filepath.Rel(BasePath, FullPath)
		if err != nil {
			return err
		}

		Mode := fi.Mode() | MODE_MASK

		if Mode.IsDir() {
			Name := fi.Name()
			for _, prefix := range IgnoredDirs {
				if Name == prefix {
					return filepath.SkipDir
				}
			}

			d := Dir{Path, Mode}
			Dirs = append(Dirs, d)
		} else if Mode.IsRegular() {
			Size := fi.Size()
			NumBlocks := Size / int64(BlockSize)
			if Size%int64(BlockSize) != 0 {
				NumBlocks++
			}
			BlockIndexEnd := BlockIndex + NumBlocks

			f := File{Path, Mode, Size, BlockIndex, BlockIndexEnd}
			Files = append(Files, f)

			BlockIndex += NumBlocks
		} else if Mode&os.ModeSymlink > 0 {
			Target, err := os.Readlink(FullPath)
			if err != nil {
				return err
			}
			s := Symlink{Path, Mode, Target}
			Symlinks = append(Symlinks, s)
		}

		return nil
	}

	err := filepath.Walk(BasePath, onEntry)
	if err != nil {
		return nil, err
	}

	NumBlocks := BlockIndex
	info := &RepoInfo{BlockSize, NumBlocks, Dirs, Symlinks, Files}
	return info, nil
}

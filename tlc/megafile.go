// Package tlc allows treating entire directory structure as a
// single file aligned on a fixed block size
package tlc

import (
	"os"
	"path/filepath"
)

var IgnoredDirs = []string{
	".git",
	".cvs",
	".svn",
}

// Regular files with data, what we actually want to send
type File struct {
	Path string
	Mode os.FileMode

	Size          int64
	BlockIndex    int64
	BlockIndexEnd int64
}

// Directories are empty directories we
type Dir struct {
	Path string
	Mode os.FileMode
}

type Symlink struct {
	Path string
	Mode os.FileMode

	Dest string
}

type RepoInfo struct {
	// Block size to align files
	BlockSize int

	// Total number of blocks
	NumBlocks int64

	// All directories, empty or not, in any order
	Dirs []Dir

	// All symlinks, in any order
	Symlinks []Symlink

	// All files, as if they were padded & concatenated
	// so that they're all aligned on a N-boundary where N is the blocksize
	Files []File
}

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

		Mode := fi.Mode()

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

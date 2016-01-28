// Package tlc allows treating entire directory structure as a
// single file where file data is aligned on a fixed block size
package tlc

import "os"

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

// Symlinks are handled separately
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

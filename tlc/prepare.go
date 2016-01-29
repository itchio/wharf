package tlc

import (
	"os"
	"path/filepath"
)

func (c *Container) Prepare(basePath string) error {
	for _, dirEntry := range c.Dirs {
		fullPath := filepath.Join(basePath, dirEntry.Path)
		tlcprint("mkdir -p %s %d", fullPath, dirEntry.Mode)
		err := os.MkdirAll(fullPath, dirEntry.Mode)
		if err != nil {
			return err
		}
		err = os.Chmod(fullPath, dirEntry.Mode)
		if err != nil {
			return err
		}
	}

	for _, fileEntry := range c.Files {
		fullPath := filepath.Join(basePath, fileEntry.Path)
		tlcprint("touch %s %d", fullPath, fileEntry.Mode)
		file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC, fileEntry.Mode)
		if err != nil {
			return err
		}
		err = file.Close()
		if err != nil {
			return err
		}

		err = os.Chmod(fullPath, fileEntry.Mode)
		if err != nil {
			return err
		}
	}

	for _, link := range c.Symlinks {
		fullPath := filepath.Join(basePath, link.Path)
		tlcprint("ln -s %s %s", link.Dest, fullPath)
		err := os.Symlink(link.Dest, fullPath)
		if err != nil {
			return err
		}
	}

	return nil
}

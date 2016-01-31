package tlc

import (
	"os"
	"path/filepath"
)

func (c *Container) Prepare(basePath string) error {
	for _, dirEntry := range c.Dirs {
		fullPath := filepath.Join(basePath, dirEntry.Path)
		err := os.MkdirAll(fullPath, os.FileMode(dirEntry.Mode))
		if err != nil {
			return err
		}
		err = os.Chmod(fullPath, os.FileMode(dirEntry.Mode))
		if err != nil {
			return err
		}
	}

	for _, fileEntry := range c.Files {
		fullPath := filepath.Join(basePath, fileEntry.Path)
		file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC, os.FileMode(fileEntry.Mode))
		if err != nil {
			return err
		}
		err = file.Close()
		if err != nil {
			return err
		}

		// if file already exists, opening with O_TRUNC doesn't change its permissions
		err = os.Chmod(fullPath, os.FileMode(fileEntry.Mode))
		if err != nil {
			return err
		}
	}

	for _, link := range c.Symlinks {
		fullPath := filepath.Join(basePath, link.Path)
		err := os.Symlink(link.Dest, fullPath)
		if err != nil {
			return err
		}
	}

	return nil
}

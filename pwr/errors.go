package pwr

import "errors"

var (
	ErrUnknownPatchVersion = errors.New("Unknown patch version")
	ErrUnknownCompression   = errors.New("Unknown compression scheme")
)

package pwr

import "errors"

var (
	ErrUnknownRecipeVersion = errors.New("Unknown recipe version")
	ErrUnknownCompression   = errors.New("Unknown compression scheme")
)

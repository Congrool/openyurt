package fs

import "errors"

var (
	ErrIsNotDir    = errors.New("the path is not a directory")
	ErrIsNotFile   = errors.New("the path is not a regular file")
	ErrExists      = errors.New("path has already existed")
	ErrNotExists   = errors.New("path does not exist")
	ErrInvalidPath = errors.New("invalid path")
)

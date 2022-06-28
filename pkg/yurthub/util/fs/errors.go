package fs

import "errors"

var (
	ErrIsNotDir  = errors.New("the path is a directory")
	ErrIsNotFile = errors.New("the path is a regular file")
	ErrExists    = errors.New("path has already existed")
	ErrNotExists = errors.New("file at path does not exist")
)

package errors

import "errors"

// ErrStorageAccessConflict is an error for accessing key conflict
var ErrStorageAccessConflict = errors.New("specified key is under accessing")

// ErrStorageNotFound is an error for not found accessing key
var ErrStorageNotFound = errors.New("specified key is not found")

// ErrKeyHasNoContent is an error for file key that has no contents
var ErrKeyHasNoContent = errors.New("specified key has no contents")

// ErrKeyIsEmpty is an error for key is empty
var ErrKeyIsEmpty = errors.New("specified key is empty")

// ErrRootKeyInvalid is an error for root key is invalid
var ErrRootKeyInvalid = errors.New("root key is invalid")

// ErrKeyExists indicates that this key has already existed
var ErrKeyExists = errors.New("specified key has already existed")

// ErrUpdateConflict indicates that using an old object to update a new object
var ErrUpdateConflict = errors.New("update conflict for old resource version")

// ErrUnrecognizedKey indicates that this key cannot be recognized by this store
var ErrUnrecognizedKey = errors.New("unrecognized key")

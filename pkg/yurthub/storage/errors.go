package storage

import "errors"

// ErrStorageAccessConflict is an error for accessing key conflict
var ErrStorageAccessConflict = errors.New("specified key is under accessing")

// ErrStorageNotFound is an error for not found accessing key
var ErrStorageNotFound = errors.New("specified key is not found")

// ErrKeyHasNoContent is an error for file key that has no contents
var ErrKeyHasNoContent = errors.New("specified key has no contents")

// ErrKeyIsEmpty is an error for key is empty
var ErrKeyIsEmpty = errors.New("specified key is empty")

// ErrInvalidContent is an error for root key is invalid
var ErrInvalidContent = errors.New("root key is invalid")

// ErrKeyExists indicates that this key has already existed
var ErrKeyExists = errors.New("specified key has already existed")

// ErrIsNotRootKey indicates that this key is not a root key
var ErrIsNotRootKey = errors.New("key is not a root key")

// ErrIsNotObjectKey indicates that this key is not a key point to a single object.
var ErrIsNotObjectKey = errors.New("key is not an object key")

// ErrUpdateConflict indicates that using an old object to update a new object
var ErrUpdateConflict = errors.New("update conflict for old resource version")

// ErrUnrecognizedKey indicates that this key cannot be recognized by this store
var ErrUnrecognizedKey = errors.New("unrecognized key")

// ErrEmptyComponent indicates that the component is empty.
var ErrEmptyComponent = errors.New("component is empty")

// ErrEmptyResource indicates that the resource is empty.
var ErrEmptyResource = errors.New("resource is empty")

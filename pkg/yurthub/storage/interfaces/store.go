/*
Copyright 2020 The OpenYurt Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package interfaces

import (
	"context"
)

// Store is an interface for caching data into backend storage
//
// Note:
// The description for each function in this interface only contains
// the interface-related error, which means other errors are also possibily returned,
// such as errors when reading/opening files.
type Store interface {
	Name() string
	// Create will create key with content in contents.
	// If key is empty, ErrKeyIsEmpty will be returned.
	// If contents is empty, ErrKeyHasNoContent will be returned.
	// If this key has already existed in this store, ErrKeyExists will be returned.
	Create(key Key, contents []byte) error

	// Delete will delete key in this store.
	// If key is empty, ErrKeyIsEmpty will be returned.
	Delete(key Key) error

	// Get will get the content of key.
	// If key is empty, ErrKeyIsEmpty will be returned.
	// If this key does not exist in this store, ErrStorageNotFound will be returned.
	Get(key Key) ([]byte, error)

	// ListKeys will retrieve all keys with the prefix of rootKey.
	// If rootKey is empty, ErrKeyIsEmpty will be returned.
	// If no keys has the prefix of rootKey, ErrStorageNotFound will be returned.
	ListKeys(rootKey Key) ([]Key, error)

	// List will retrieve all contents whose keys have the prefix of rootKey.
	// If rootKey is empty, ErrKeyIsEmpty will be returned.
	// If no keys has the prefix of rootKey, ErrStorageNotFound will be returned.
	List(rootKey Key) ([][]byte, error)

	// Update will try to update key in store with passed-in contents. Only when
	// the rv of passed-in contents is fresher than what is in the store, the Update will happen.
	// If force is set as true, rv will be ignored, using the passed-in contents to
	// replace what is in the store.
	// If key is empty, ErrKeyIsEmpty will be returned.
	// If the contents is empty, ErrKeyHasNoContent will be returned.
	// If force is not set and the rv is staler than what is in the store, ErrUpdateConflict will be returned.
	// If the key does not exist in the store, ErrStorageNotFound will be returned.
	Update(key Key, contents []byte, rv uint64, force bool) ([]byte, error)

	// UpdateList will try to update all keys with prefix of rootKey. For each content, only when
	// its rv in rvs is fresher than what is in the store, the Update will happen. Otherwise it will
	// remain as it is. Selector is an additional info for disk storage.
	// TODO: reconsider the selector parameter, it's not generic.
	// If rootKey is empty, ErrKeyIsEmpty will be returned.
	// If any key of contents exists that does not have the prefix of rootKey, ErrRootKeyInvalid will be returned.
	UpdateList(rootKey Key, contents map[Key][]byte, rvs map[Key]int64, selector string) error

	// DeleteCollection will delete all the keys have the prefix of rootKey.
	// If rootKey is empty, ErrKeyIsEmpty will be returned.
	DeleteCollection(rootKey Key) error

	// KeyFunc will get the Key used by this store
	// reqCtx is the context with RequestInfo, which contains info needed when
	// generating the key, including namespace and name. If namespace and name are
	// provided as arguments, they will be used instead of what in the RequestInfo.
	KeyFunc(reqCtx context.Context, namespace, name string) (Key, error)

	// TODO: RootKeyFunc()
	// decouple key with root key
}

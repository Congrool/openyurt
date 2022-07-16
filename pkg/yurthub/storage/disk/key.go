package disk

import (
	"path/filepath"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

type storageKey struct {
	isRootKey bool
	path      string
}

func (k storageKey) Key() string {
	return k.path
}

func (k storageKey) IsRootKey() bool {
	return k.isRootKey
}

// Key for disk storage is
// /<Component>/<Resource>/<Namespace>/<Name>, or
// /<Component>/<Resource>/<Name>, if there's no namespace provided in info.
// /<Component>/<Resource>/<Namespace>, if there's no name provided in info.
// /<Component>/<Resource>, if there's no namespace and name provided in info.
func (ds *diskStorage) KeyFunc(info storage.KeyBuildInfo) (storage.Key, error) {
	isRoot := false
	if info.Component == "" {
		return nil, storage.ErrEmptyComponent
	}
	if info.Resources == "" {
		return nil, storage.ErrEmptyResource
	}
	if info.Name == "" {
		isRoot = true
	}

	return storageKey{
		path:      filepath.Join(info.Component, info.Resources, info.Namespace, info.Name),
		isRootKey: isRoot,
	}, nil
}

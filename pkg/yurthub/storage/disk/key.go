package disk

import (
	"fmt"
	"path/filepath"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

type storageKey string

func (k storageKey) Key() string {
	return string(k)
}

var emptyStorageKey = storageKey("")

// KeyFunc will try to use namespace and name in ctx. If namespace and name are
// provided in parameters, it will use them instead.
// Key for disk storage is
// /<Component>/<Resource>/<Namespace>/<Name>, or
// /<Component>/<Resource>/<Name>, if there's no namespace,
// /<Component>/<Resource>, if it's a list object.
func (ds *diskStorage) KeyFunc(info storage.KeyBuildInfo) (storage.Key, error) {
	var comp, res, ns, n string
	ns, n = info.Namespace, info.Name
	if err := MustSet(&comp, info.Component); err != nil {
		return nil, fmt.Errorf("failed to set component for key, %s", err)
	}
	if err := MustSet(&res, info.Resources); err != nil {
		return nil, fmt.Errorf("failed to set resource for key, %s", err)
	}
	return storageKey(filepath.Join(comp, res, ns, n)), nil
}

// MustSet will ensure that str must be set. If str is empty, it will be set as
// candidateVal. If even candidateVal is empty, it will return error.
func MustSet(str *string, candidateVal string) error {
	if *str != "" {
		return nil
	}
	*str = candidateVal
	if *str != "" {
		return nil
	}
	return fmt.Errorf("value cannot be empty")
}

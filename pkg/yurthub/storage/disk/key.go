package disk

import (
	"context"
	"fmt"
	"path/filepath"

	apirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage/interfaces"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"
)

type storageKey string

func (k storageKey) Key() string {
	return string(k)
}

// UnsafeDiskStorageKey will use passed-in key as the value of
// StorageKey, there's no validation. If using this function, you
// should know what you're doing.
func UnsafeDiskStorageKey(key string) interfaces.Key {
	return storageKey(key)
}

var emptyStorageKey = storageKey("")

// KeyFunc will try to use namespace and name in ctx. If namespace and name are
// provided in parameters, it will use them instead.
// Key for disk storage is
// /<Component>/<Resource>/<Namespace>/<Name>, or
// /<Component>/<Resource>/<Name>, if there's no namespace,
// /<Component>/<Resource>, if it's a list object.
func (ds *diskStorage) KeyFunc(ctx context.Context, namespace, name string) (interfaces.Key, error) {
	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok || info == nil {
		return nil, fmt.Errorf("failed to get request info")
	}
	component, ok := util.ClientComponentFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("failed to get component info")
	}

	var comp, res, ns, n string
	ns, n = info.Namespace, info.Name
	if err := MustSet(&comp, component); err != nil {
		return nil, fmt.Errorf("failed to set component for key, %s", err)
	}
	if err := MustSet(&res, info.Resource); err != nil {
		return nil, fmt.Errorf("failed to set resource for key, %s", err)
	}
	if namespace != "" && namespace != ns {
		ns = namespace
	}
	if name != "" && name != n {
		n = name
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

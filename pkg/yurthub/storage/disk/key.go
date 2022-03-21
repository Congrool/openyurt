package disk

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/openyurtio/openyurt/pkg/yurthub/util"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// KeyFunc will try to use namespace and name in ctx. If namespace and name are
// provided in parameters, it will use them instead.
// Key for disk storage is
// /<Component>/<Resource>/<Namespace>/<Name>, or
// /<Component>/<Resource>/<Name>, if there's no namespace,
// /<Component>/<Resource>, if it's a list object.
func KeyFunc(ctx context.Context, namespace, name string) (string, error) {
	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok || info == nil {
		return "", fmt.Errorf("failed to get request info")
	}
	component, ok := util.ClientComponentFrom(ctx)
	if !ok {
		return "", fmt.Errorf("failed to get component info")
	}

	var comp, res, ns, n string
	ns, n = info.Namespace, info.Name
	if err := MustSet(&comp, component); err != nil {
		return "", fmt.Errorf("failed to set component for key, %s", err)
	}
	if err := MustSet(&res, info.Resource); err != nil {
		return "", fmt.Errorf("failed to set resource for key, %s", err)
	}
	if namespace != "" && namespace != ns {
		ns = namespace
	}
	if name != "" && name != n {
		n = name
	}

	return filepath.Join(comp, res, ns, n), nil
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

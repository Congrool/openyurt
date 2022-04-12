package poolspirit

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/validation/path"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

func (s *Storage) GetKeyFunc() storage.KeyFunc {
	return s.KeyFunc
}

// KeyFunc will try to use namespace and name in ctx. If namespace and name are
// provided in parameters, it will use them instead.
// For signal object:
// /<Prefix>/<Resource>/<Namespace>/<Name>, or
// /<Prefix>/<Resource>/<Name>, if the obj is non-namespaced,
//
// For list object:
// /<prefix>/<Resource>/<Namespace>, or
// /<prefix>/<Resource>
//
// TODO: minions for node resource
// Note: for node resource, <Resource> will be minons.
func (s *Storage) KeyFunc(ctx context.Context, namespace, name string) (string, error) {
	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok || info == nil {
		return "", fmt.Errorf("failed to get request info")
	}

	res := info.Resource
	if res == "" {
		return "", fmt.Errorf("failed to get resource")
	}

	if res == "nodes" {
		// kubernetes stores minions instead of nodes in etcd
		res = "minions"
	}

	prefix := s.prefix + "/" + res
	if info.Namespace == "" && namespace == "" {
		if name == "" {
			return NoNamespaceKeyFunc(prefix, info.Name)
		} else {
			return NoNamespaceKeyFunc(prefix, name)
		}
	}

	ns := info.Namespace
	if namespace != "" {
		ns = namespace
	}
	if name == "" {
		return NamespaceKeyFunc(prefix, ns, info.Name)
	}
	return NamespaceKeyFunc(prefix, ns, name)
}

// NamespaceKeyRootFunc is the default function for constructing storage paths
// to resource directories enforcing namespace rules.
func NamespaceKeyRootFunc(prefix string, namespace string) string {
	key := prefix
	if len(namespace) > 0 {
		key = key + "/" + namespace
	}
	return key
}

// NamespaceKeyFunc is the default function for constructing storage paths to
// a resource relative to the given prefix enforcing namespace rules.
func NamespaceKeyFunc(prefix string, namespace string, name string) (string, error) {
	key := NamespaceKeyRootFunc(prefix, namespace)
	if len(namespace) == 0 {
		return "", fmt.Errorf("namespace parameter required")
	}
	if msgs := path.IsValidPathSegmentName(name); len(msgs) != 0 {
		return "", fmt.Errorf(fmt.Sprintf("Name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key = key + "/" + name
	return key, nil
}

// NoNamespaceKeyFunc is the default function for constructing storage paths
// to a resource relative to the given prefix without a namespace.
func NoNamespaceKeyFunc(prefix string, name string) (string, error) {
	if msgs := path.IsValidPathSegmentName(name); len(msgs) != 0 {
		return "", fmt.Errorf(fmt.Sprintf("name parameter invalid: %q: %s", name, strings.Join(msgs, ";")))
	}
	key := prefix + "/" + name
	return key, nil
}

package poolspirit

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/validation/path"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

// SpecialDefaultResourcePrefixes are prefixes compiled into Kubernetes.
// refer to SpecialDefaultResourcePrefixes in k8s.io/pkg/kubeapiserver/default_storage_factory_builder.go
var SpecialDefaultResourcePrefixes = map[schema.GroupResource]string{
	{Group: "", Resource: "replicationcontrollers"}:        "controllers",
	{Group: "", Resource: "endpoints"}:                     "services/endpoints",
	{Group: "", Resource: "nodes"}:                         "minions",
	{Group: "", Resource: "services"}:                      "services/specs",
	{Group: "extensions", Resource: "ingresses"}:           "ingress",
	{Group: "networking.k8s.io", Resource: "ingresses"}:    "ingress",
	{Group: "extensions", Resource: "podsecuritypolicies"}: "podsecuritypolicy",
	{Group: "policy", Resource: "podsecuritypolicies"}:     "podsecuritypolicy",
}

func (s *Storage) GetKeyFunc() storage.KeyFunc {
	return s.KeyFunc
}

// KeyFunc will try to use namespace and name in ctx. If namespace and name are
// provided in parameters, it will use them instead.
// For singal object:
// /<Prefix>/<Resource>/<Namespace>/<Name>, or
// /<Prefix>/<Resource>/<Name>, if the obj is non-namespaced,
//
// For list object:
// /<prefix>/<Resource>/<Namespace>, or
// /<prefix>/<Resource>
func (s *Storage) KeyFunc(ctx context.Context, namespace, name string) (string, error) {
	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok || info == nil {
		return "", fmt.Errorf("failed to get request info")
	}

	res := info.Resource
	if res == "" {
		return "", fmt.Errorf("failed to get resource")
	}

	resourcePrefix := res
	groupResource := schema.GroupResource{Group: info.APIGroup, Resource: res}
	if overridePrefix, ok := SpecialDefaultResourcePrefixes[groupResource]; ok {
		resourcePrefix = overridePrefix
	}
	prefix := s.prefix + "/" + resourcePrefix

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

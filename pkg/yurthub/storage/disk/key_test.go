package disk

import (
	"os"
	"testing"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

var keyFuncTestDir = "/tmp/oy-diskstore-keyfunc"

func TestKeyFunc(t *testing.T) {
	cases := map[string]struct {
		info   storage.KeyBuildInfo
		key    string
		err    error
		isRoot bool
	}{
		"namespaced resource key": {
			info: storage.KeyBuildInfo{
				Component: "kubelet",
				Resources: "pods",
				Namespace: "kube-system",
				Name:      "kube-proxy-xx",
			},
			key:    "kubelet/pods/kube-system/kube-proxy-xx",
			isRoot: false,
		},
		"non-namespaced resource key": {
			info: storage.KeyBuildInfo{
				Component: "kubelet",
				Resources: "nodes",
				Name:      "edge-worker",
			},
			key:    "kubelet/nodes/edge-worker",
			isRoot: false,
		},
		"resource list key": {
			info: storage.KeyBuildInfo{
				Component: "kubelet",
				Resources: "pods",
			},
			key:    "kubelet/pods",
			isRoot: true,
		},
		"resource list namespace key": {
			info: storage.KeyBuildInfo{
				Component: "kube-proxy",
				Resources: "services",
				Namespace: "default",
			},
			key:    "kube-proxy/services/default",
			isRoot: true,
		},
		"no component err key": {
			info: storage.KeyBuildInfo{
				Resources: "nodes",
			},
			err: storage.ErrEmptyComponent,
		},
		"no resource err key": {
			info: storage.KeyBuildInfo{
				Component: "kubelet",
				Name:      "edge-worker",
			},
			err: storage.ErrEmptyResource,
		},
	}

	disk, err := NewDiskStorage(keyFuncTestDir)
	if err != nil {
		t.Errorf("failed to create disk store, %v", err)
		return
	}
	keyFunc := disk.KeyFunc
	for c, s := range cases {
		key, err := keyFunc(s.info)
		if err != s.err {
			t.Errorf("unexpected err for case: %s, want: %s, got: %s", c, err, s.err)
			continue
		}

		if err != nil {
			continue
		}

		if key.Key() != s.key {
			t.Errorf("unexpected key for case: %s, want: %s, got: %s", c, s.key, key.Key())
			continue
		}

		if key.IsRootKey() != s.isRoot {
			t.Errorf("unexpected key type for case: %s, want: %v, got: %v", c, s.isRoot, key.IsRootKey())
		}
	}
	os.RemoveAll(keyFuncTestDir)
}

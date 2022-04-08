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

package cachemanager

import (
	"bytes"
	"strconv"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"
)

// StorageWrapper is wrapper for storage.Store interface
// in order to handle serialize runtime object
type StorageWrapper interface {
	Create(key string, obj runtime.Object) error
	Delete(key string) error
	Get(key string) (runtime.Object, error)
	ListKeys(key string) ([]string, error)
	List(key string) ([]runtime.Object, error)
	Update(key string, obj runtime.Object, rv uint64, force bool) (runtime.Object, error)
	Replace(rootKey string, objs map[string]runtime.Object) error
	DeleteCollection(rootKey string) error
	GetRaw(key string) ([]byte, error)
	UpdateRaw(key string, contents []byte, rv uint64) ([]byte, error)
}

type storageWrapper struct {
	sync.RWMutex
	store             storage.Store
	backendSerializer runtime.Serializer
	cache             map[string]runtime.Object
}

// NewStorageWrapper create a StorageWrapper object
func NewStorageWrapper(storage storage.Store) StorageWrapper {
	return &storageWrapper{
		store:             storage,
		backendSerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, scheme.Scheme, scheme.Scheme, json.SerializerOptions{}),
		cache:             make(map[string]runtime.Object),
	}
}

// Create store runtime object into backend storage
// if obj is nil, the storage used to represent the key
// will be created. for example: for disk storage,
// a directory that indicates the key will be created.
func (sw *storageWrapper) Create(key string, obj runtime.Object) error {
	var buf bytes.Buffer
	if obj != nil {
		if err := sw.backendSerializer.Encode(obj, &buf); err != nil {
			klog.Errorf("failed to encode object in create for %s, %v", key, err)
			return err
		}
	}

	if err := sw.store.Create(key, buf.Bytes()); err != nil {
		return err
	}

	if obj != nil && isCacheKey(key) {
		sw.Lock()
		sw.cache[key] = obj
		sw.Unlock()
	}

	return nil
}

// Delete remove runtime object that by specified key from backend storage
func (sw *storageWrapper) Delete(key string) error {
	if err := sw.store.Delete(key); err != nil {
		return err
	}

	if isCacheKey(key) {
		sw.Lock()
		delete(sw.cache, key)
		sw.Unlock()
	}

	return nil
}

// Get get the runtime object that specified by key from backend storage
func (sw *storageWrapper) Get(key string) (runtime.Object, error) {
	cachedKey := isCacheKey(key)
	if cachedKey {
		sw.RLock()
		cachedObject, ok := sw.cache[key]
		sw.RUnlock()
		if ok && cachedObject != nil {
			return cachedObject, nil
		}
	}

	b, err := sw.GetRaw(key)
	if err != nil {
		return nil, err
	} else if len(b) == 0 {
		return nil, nil
	}
	//get the gvk from json data
	gvk, err := json.DefaultMetaFactory.Interpret(b)
	if err != nil {
		return nil, err
	}
	var UnstructuredObj runtime.Object
	if scheme.Scheme.Recognizes(*gvk) {
		UnstructuredObj = nil
	} else {
		UnstructuredObj = new(unstructured.Unstructured)
	}
	obj, gvk, err := sw.backendSerializer.Decode(b, nil, UnstructuredObj)
	if err != nil {
		klog.Errorf("could not decode %v for %s, %v", gvk, key, err)
		return nil, err
	}

	if cachedKey {
		sw.Lock()
		sw.cache[key] = obj
		sw.Unlock()
	}
	return obj, nil
}

// ListKeys list all keys with key as prefix
func (sw *storageWrapper) ListKeys(key string) ([]string, error) {
	return sw.store.ListKeys(key)
}

// List get all of runtime objects that specified by key as prefix
func (sw *storageWrapper) List(key string) ([]runtime.Object, error) {
	bb, err := sw.store.List(key)
	objects := make([]runtime.Object, 0, len(bb))
	if err != nil {
		klog.Errorf("could not list objects for %s, %v", key, err)
		return nil, err
	} else if len(bb) == 0 {
		if isPodKey(key) {
			// because at least there will be yurt-hub pod on the node.
			// if no pods in cache, maybe all of pods have been deleted by accident,
			// if empty object is returned, pods on node will be deleted by kubelet.
			// in order to prevent the influence to business, return error here so pods
			// will be kept on node.
			return objects, storage.ErrStorageNotFound
		}
		return objects, nil
	}
	//get the gvk from json data
	gvk, err := json.DefaultMetaFactory.Interpret(bb[0])
	if err != nil {
		return nil, err
	}
	var UnstructuredObj runtime.Object
	var recognized bool
	if scheme.Scheme.Recognizes(*gvk) {
		recognized = true
	}

	for i := range bb {
		if !recognized {
			UnstructuredObj = new(unstructured.Unstructured)
		}

		obj, gvk, err := sw.backendSerializer.Decode(bb[i], nil, UnstructuredObj)
		if err != nil {
			klog.Errorf("could not decode %v for %s, %v", gvk, key, err)
			continue
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

// Update update runtime object in backend storage
func (sw *storageWrapper) Update(key string, obj runtime.Object, rv uint64, force bool) (runtime.Object, error) {
	var buf bytes.Buffer
	if err := sw.backendSerializer.Encode(obj, &buf); err != nil {
		klog.Errorf("failed to encode object in update for %s, %v", key, err)
		return nil, err
	}

	content, err := sw.UpdateRaw(key, buf.Bytes(), rv)
	if err != nil {
		if err == storage.ErrUpdateConflict {
			out := new(unstructured.Unstructured)
			gvk := obj.GetObjectKind().GroupVersionKind()
			if scheme.Scheme.Recognizes(gvk) {
				out = nil
			}
			curobj, _, err := sw.backendSerializer.Decode(content, nil, out)
			if err != nil {
				klog.Errorf("failed to decode content for %s, %v", key, err)
				return nil, err
			}
			if isCacheKey(key) {
				sw.Lock()
				sw.cache[key] = curobj
				sw.Unlock()
			}
			return curobj, nil
		}
		return nil, err
	}

	if isCacheKey(key) {
		sw.Lock()
		sw.cache[key] = obj
		sw.Unlock()
	}

	return nil, nil
}

// TODO: update comment
// Replace will delete the old objects, and use the given objs instead.
func (sw *storageWrapper) Replace(rootKey string, objs map[string]runtime.Object) error {
	var buf bytes.Buffer
	contents := make(map[string][]byte, len(objs))
	rvs := make(map[string]int64, len(objs))
	for key, obj := range objs {
		if err := sw.backendSerializer.Encode(obj, &buf); err != nil {
			klog.Errorf("failed to encode object in update for %s, %v", key, err)
			return err
		}
		accessor := meta.NewAccessor()
		rv, err := accessor.ResourceVersion(obj)
		if err != nil {
			klog.Warningf("failed to get resource version for %s, %v, continue with 0", key, err)
			rv = "0"
		}
		rvs[key], err = strconv.ParseInt(rv, 10, 64)
		if err != nil {
			klog.Errorf("failed to parse resource version for %s, rv is %s, %v", key, rv, err)
			return err
		}
		contents[key] = make([]byte, len(buf.Bytes()))
		copy(contents[key], buf.Bytes())
		buf.Reset()
	}

	return sw.store.Replace(rootKey, contents, rvs)
}

// DeleteCollection will delete all objects under rootKey
func (sw *storageWrapper) DeleteCollection(rootKey string) error {
	return sw.store.DeleteCollection(rootKey)
}

// GetRaw get byte data for specified key
func (sw *storageWrapper) GetRaw(key string) ([]byte, error) {
	return sw.store.Get(key)
}

// UpdateRaw update contents(byte date) for specified key
func (sw *storageWrapper) UpdateRaw(key string, contents []byte, rv uint64) ([]byte, error) {
	return sw.store.Update(key, contents, rv, false)
}

// isCacheKey verify runtime object is cached for specified key.
// in order to accelerate kubelet get node and lease object, we cache them
func isCacheKey(key string) bool {
	comp, resource, _, _ := util.SplitKey(key)
	switch {
	case comp == "kubelet" && resource == "nodes":
		return true
	case comp == "kubelet" && resource == "leases":
		return true
	}

	return false
}

// isPodKey verify the key is kubelet/pods or not
func isPodKey(key string) bool {
	comp, resource, _, _ := util.SplitKey(key)
	return comp == "kubelet" && resource == "pods"
}

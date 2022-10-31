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
	"fmt"
	"strconv"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

// StorageWrapper is wrapper for storage.Store interface
// in order to handle serialize runtime object
type StorageWrapper interface {
	Name() string
	Create(key storage.Key, obj runtime.Object) error
	Delete(key storage.Key) error
	Get(key storage.Key) (runtime.Object, error)
	List(key storage.Key) ([]runtime.Object, error)
	Update(key storage.Key, obj runtime.Object, rv uint64) (runtime.Object, error)
	KeyFunc(info storage.KeyBuildInfo) (storage.Key, error)
	ListResourceKeysOfComponent(component string, resource string) ([]storage.Key, error)
	ReplaceComponentList(component, resource, namespace string, contents map[storage.Key]runtime.Object) error
	DeleteComponentResources(component string) error
}

type storageWrapper struct {
	sync.RWMutex
	store             storage.Store
	backendSerializer runtime.Serializer
}

// NewStorageWrapper create a StorageWrapper object
func NewStorageWrapper(storage storage.Store) StorageWrapper {
	return &storageWrapper{
		store:             storage,
		backendSerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, scheme.Scheme, scheme.Scheme, json.SerializerOptions{}),
	}
}

func (sw *storageWrapper) Name() string {
	return sw.store.Name()
}

func (sw *storageWrapper) KeyFunc(info storage.KeyBuildInfo) (storage.Key, error) {
	return sw.store.KeyFunc(info)
}

// Create store runtime object into backend storage
// if obj is nil, the storage used to represent the key
// will be created. for example: for disk storage,
// a directory that indicates the key will be created.
func (sw *storageWrapper) Create(key storage.Key, obj runtime.Object) error {
	var buf bytes.Buffer
	if obj != nil {
		if err := sw.backendSerializer.Encode(obj, &buf); err != nil {
			klog.Errorf("failed to encode object in create for %s, %v", key.Key(), err)
			return err
		}
	}

	if err := sw.store.Create(key, buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// Delete remove runtime object that by specified key from backend storage
func (sw *storageWrapper) Delete(key storage.Key) error {
	return sw.store.Delete(key)
}

// Get get the runtime object that specified by key from backend storage
func (sw *storageWrapper) Get(key storage.Key) (runtime.Object, error) {
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
		klog.Errorf("could not decode %v for %s, %v", gvk, key.Key(), err)
		return nil, err
	}

	return obj, nil
}

// ListKeys list all keys with key as prefix
func (sw *storageWrapper) ListResourceKeysOfComponent(component string, resource string) ([]storage.Key, error) {
	return sw.store.ListResourceKeysOfComponent(component, resource)
}

// List get all of runtime objects that specified by key as prefix
func (sw *storageWrapper) List(key storage.Key) ([]runtime.Object, error) {
	bb, err := sw.store.List(key)
	objects := make([]runtime.Object, 0, len(bb))
	if err != nil {
		klog.Errorf("could not list objects for %s, %v", key.Key(), err)
		return nil, err
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
			klog.Errorf("could not decode %v for %s, %v", gvk, key.Key(), err)
			continue
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

// Update update runtime object in backend storage
func (sw *storageWrapper) Update(key storage.Key, obj runtime.Object, rv uint64) (runtime.Object, error) {
	var buf bytes.Buffer
	if err := sw.backendSerializer.Encode(obj, &buf); err != nil {
		klog.Errorf("failed to encode object in update for %s, %v", key.Key(), err)
		return nil, err
	}

	if buf, err := sw.store.Update(key, buf.Bytes(), rv); err != nil {
		if err == storage.ErrUpdateConflict {
			obj, _, dErr := sw.backendSerializer.Decode(buf, nil, nil)
			if dErr != nil {
				return nil, fmt.Errorf("failed to decode existing obj of key %s, %v", key.Key(), dErr)
			}
			return obj, err
		}
		return nil, err
	}

	return obj, nil
}

func (sw *storageWrapper) ReplaceComponentList(component, resource, namespace string, objs map[storage.Key]runtime.Object) error {
	var buf bytes.Buffer
	contents := make(map[storage.Key][]byte, len(objs))
	rvs := make(map[storage.Key]int64, len(objs))
	for key, obj := range objs {
		if err := sw.backendSerializer.Encode(obj, &buf); err != nil {
			klog.Errorf("failed to encode object in update for %s, %v", key.Key(), err)
			return err
		}
		accessor := meta.NewAccessor()
		rv, err := accessor.ResourceVersion(obj)
		if err != nil {
			klog.Warningf("failed to get resource version for %s, %v, continue with 0", key.Key(), err)
			rv = "0"
		}
		rvs[key], err = strconv.ParseInt(rv, 10, 64)
		if err != nil {
			klog.Errorf("failed to parse resource version for %s, rv is %s, %v", key.Key(), rv, err)
			return err
		}
		contents[key] = make([]byte, len(buf.Bytes()))
		copy(contents[key], buf.Bytes())
		buf.Reset()
	}

	return sw.store.ReplaceComponentList(component, resource, namespace, contents)
}

// DeleteCollection will delete all objects under rootKey
func (sw *storageWrapper) DeleteComponentResources(component string) error {
	return sw.store.DeleteComponentResources(component)
}

// GetRaw get byte data for specified key
func (sw *storageWrapper) GetRaw(key storage.Key) ([]byte, error) {
	return sw.store.Get(key)
}

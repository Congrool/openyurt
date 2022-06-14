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

package disk

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage/errors"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/interfaces"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/utils"
)

const (
	CacheBaseDir = "/etc/kubernetes/cache/"
	StorageName  = "local-disk"
	tmpPrefix    = "tmp_"
)

type diskStorage struct {
	baseDir          string
	keyPendingStatus map[string]struct{}
	serializer       runtime.Serializer
	sync.Mutex
	listSelectorCollector map[string]string
}

// NewDiskStorage creates a storage.Store for caching data into local disk
func NewDiskStorage(dir string) (interfaces.Store, error) {
	if dir == "" {
		klog.Infof("disk cache path is empty, set it by default %s", CacheBaseDir)
		dir = CacheBaseDir
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	ds := &diskStorage{
		keyPendingStatus: make(map[string]struct{}),
		baseDir:          dir,
		serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, scheme.Scheme, scheme.Scheme, json.SerializerOptions{}),
	}

	err := ds.Recover(storageKey(""))
	if err != nil {
		klog.Errorf("could not recover local storage, %v, and skip the error", err)
	}
	return ds, nil
}

func (ds *diskStorage) Name() string {
	return StorageName
}

// Create new a file with key and contents or create dir only
// when contents are empty.
func (ds *diskStorage) Create(key interfaces.Key, contents []byte) error {
	if err := utils.ValidateKey(key, emptyStorageKey); err != nil {
		return err
	}
	if !ds.lockKey(key) {
		return errors.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	// no contents, create key dir only
	if len(contents) == 0 {
		keyPath := filepath.Join(ds.baseDir, key.Key())
		if info, err := os.Stat(keyPath); err != nil {
			if os.IsNotExist(err) {
				if err = os.MkdirAll(keyPath, 0755); err != nil {
					return err
				}
				return errors.ErrKeyHasNoContent
			}
		} else if info.IsDir() {
			return errors.ErrKeyExists
		} else {
			return errors.ErrKeyHasNoContent
		}
	}

	return ds.create(key, contents)
}

// create will make up a file with key as file path and contents as file contents.
func (ds *diskStorage) create(key interfaces.Key, contents []byte) error {
	keyPath := filepath.Join(ds.baseDir, key.Key())
	dir, _ := filepath.Split(keyPath)
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(dir, 0755); err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		// dir for key is already exist
		if _, err := os.Stat(keyPath); err == nil || !os.IsNotExist(err) {
			return errors.ErrKeyExists
		}
	}

	// open file with synchronous I/O
	f, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0600)
	if err != nil {
		return err
	}
	n, err := f.Write(contents)
	if err == nil && n < len(contents) {
		err = io.ErrShortWrite
	}

	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// Delete delete file that specified by key
func (ds *diskStorage) Delete(key interfaces.Key) error {
	if err := utils.ValidateKey(key, emptyStorageKey); err != nil {
		return err
	}

	if !ds.lockKey(key) {
		return errors.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	errs := make([]error, 0)
	if err := ds.delete(key); err != nil {
		errs = append(errs, err)
	}

	tmpKey := getTmpKey(key)
	if err := ds.delete(tmpKey); err != nil {
		errs = append(errs, err)
	}

	if len(errs) != 0 {
		return fmt.Errorf("%#+v", errs)
	}
	return nil
}

func (ds *diskStorage) delete(key interfaces.Key) error {
	if err := utils.ValidateKey(key, emptyStorageKey); err != nil {
		return err
	}

	absKey := filepath.Join(ds.baseDir, key.Key())
	info, err := os.Stat(absKey)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if info.Mode().IsRegular() {
		return os.Remove(absKey)
	}

	return nil
}

// Get get contents from the file that specified by key
func (ds *diskStorage) Get(key interfaces.Key) ([]byte, error) {
	if err := utils.ValidateKey(key, emptyStorageKey); err != nil {
		return []byte{}, errors.ErrKeyIsEmpty
	}

	if !ds.lockKey(key) {
		return nil, errors.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)
	return ds.get(filepath.Join(ds.baseDir, key.Key()))
}

// get returns contents from the file of path
func (ds *diskStorage) get(path string) ([]byte, error) {
	if path == "" {
		return []byte{}, errors.ErrKeyIsEmpty
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []byte{}, errors.ErrStorageNotFound
		}
		return nil, fmt.Errorf("failed to get bytes from %s, %v", path, err)
	} else if info.Mode().IsRegular() {
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return []byte{}, err
		}

		return b, nil
	} else if info.IsDir() {
		return []byte{}, errors.ErrKeyHasNoContent
	}

	return nil, fmt.Errorf("%s is exist, but not recognized, %v", path, info.Mode())
}

// ListKeys list all of keys for files
func (ds *diskStorage) ListKeys(rootKey interfaces.Key) ([]interfaces.Key, error) {
	if err := utils.ValidateKey(rootKey, emptyStorageKey); err != nil {
		return nil, errors.ErrKeyIsEmpty
	}

	keys := make([]interfaces.Key, 0)
	absPath := filepath.Join(ds.baseDir, rootKey.Key())
	if info, err := os.Stat(absPath); err != nil {
		if os.IsNotExist(err) {
			return keys, errors.ErrStorageNotFound
		}
		return keys, err
	} else if info.IsDir() {
		err := filepath.Walk(absPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.Mode().IsRegular() {
				if !isTmpFile(path) {
					keys = append(keys, storageKey(strings.TrimPrefix(path, ds.baseDir)))
				}
			}

			return nil
		})

		return keys, err
	} else if info.Mode().IsRegular() {
		keys = append(keys, rootKey)
		return keys, nil
	}

	return keys, fmt.Errorf("failed to list keys because %s not recognized", rootKey)
}

// List get all of contents for local files
func (ds *diskStorage) List(key interfaces.Key) ([][]byte, error) {
	if err := utils.ValidateKey(key, emptyStorageKey); err != nil {
		return [][]byte{}, errors.ErrKeyIsEmpty
	}

	if !ds.lockKey(key) {
		return nil, errors.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	bb := make([][]byte, 0)
	absKey := filepath.Join(ds.baseDir, key.Key())
	info, err := os.Stat(absKey)
	if err != nil {
		if os.IsNotExist(err) {
			return bb, errors.ErrStorageNotFound
		}
		klog.Errorf("filed to list bytes for (%s), %v", key.Key(), err)
		return nil, err
	} else if info.Mode().IsRegular() {
		b, err := ds.get(absKey)
		if err != nil {
			return nil, err
		}

		bb = append(bb, b)
		return bb, nil
	} else if info.Mode().IsDir() {
		err := filepath.Walk(absKey, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.Mode().IsRegular() && !isTmpFile(path) {
				b, err := ds.get(path)
				if err != nil {
					klog.Warningf("failed to get bytes for %s when listing bytes, %v", path, err)
					return nil
				}

				bb = append(bb, b)
			}

			return nil
		})

		if err != nil {
			return nil, err
		}

		return bb, nil
	}

	return nil, fmt.Errorf("%s is exist, but not recognized, %v", key.Key(), info.Mode())
}

// Update will update local file that specified by key with contents
func (ds *diskStorage) Update(key interfaces.Key, contents []byte, rv uint64, force bool) ([]byte, error) {
	if err := utils.ValidateKV(key, contents, emptyStorageKey); err != nil {
		return nil, err
	}

	if !ds.lockKey(key) {
		return nil, errors.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	keyStr := key.Key()
	curContent, err := ds.get(filepath.Join(ds.baseDir, keyStr))
	if err != nil {
		return nil, err
	}
	klog.V(4).Infof("find key %s exist when updating it", keyStr)
	if !force {
		// check resource version
		unstructuredObj := &unstructured.Unstructured{}
		curObj, _, err := ds.serializer.Decode(curContent, nil, unstructuredObj)
		if err != nil {
			klog.Errorf("failed to decode obj at %s, %v", keyStr, err)
			return nil, err
		}
		curRv, err := ObjectResourceVersion(curObj)
		if err != nil {
			klog.Errorf("failed to get rv of obj exists at %s, %v", keyStr, err)
			return nil, err
		}
		if rv < curRv {
			klog.V(4).Infof("find that obj with key %s has higher rv, do not update it", keyStr)
			return nil, errors.ErrUpdateConflict
		}
	}

	// 1. create new file with tmpKey
	tmpKey := getTmpKey(key)
	if err := ds.create(tmpKey, contents); err != nil {
		return nil, err
	}

	// 2. delete old file by key
	if err := ds.delete(key); err != nil {
		ds.delete(tmpKey)
		return nil, err
	}

	// 3. rename tmpKey file to key file
	return nil, os.Rename(filepath.Join(ds.baseDir, tmpKey.Key()), filepath.Join(ds.baseDir, keyStr))
}

// UpdateList will update files under rootKey dir with new files provided in contents.
// Note: when the contents are empty and the dir already exists, the create function will clean the current dir
func (ds *diskStorage) UpdateList(rootKey interfaces.Key, contents map[interfaces.Key][]byte, _ map[interfaces.Key]int64, selector string) error {
	if err := utils.ValidateKey(rootKey, emptyStorageKey); err != nil {
		return err
	}

	if _, err := ds.canUpdateList(rootKey, selector); err != nil {
		return fmt.Errorf("cannot update list of root key %s, %v", rootKey.Key(), err)
	}

	for key := range contents {
		if !strings.Contains(key.Key(), rootKey.Key()) {
			return errors.ErrRootKeyInvalid
		}
	}

	if !ds.lockKey(rootKey) {
		return errors.ErrStorageAccessConflict
	}
	defer ds.unLockKey(rootKey)

	// 1. mv old dir into tmp_dir when rootKey dir already exists
	absPath := filepath.Join(ds.baseDir, rootKey.Key())
	tmpRootKey := getTmpKey(rootKey)
	tmpPath := filepath.Join(ds.baseDir, tmpRootKey.Key())
	dirExisted := false
	if info, err := os.Stat(absPath); err == nil {
		if info.IsDir() {
			err := os.Rename(absPath, tmpPath)
			if err != nil {
				return err
			}
			dirExisted = true
		}
	}

	// 2. create new file with contents
	// TODO: if error happens, we may need retry mechanism, or add some mechanism to do consistency check.
	if len(contents) != 0 {
		for key, data := range contents {
			err := ds.create(key, data)
			if err != nil {
				klog.Errorf("failed to create %s in replace, %v", key, err)
				continue
			}
		}
	}

	//  3. delete old tmp dir
	if dirExisted {
		return os.RemoveAll(tmpPath)
	}

	return nil
}

// DeleteCollection delete file or dir that specified by rootKey
func (ds *diskStorage) DeleteCollection(rootKey interfaces.Key) error {
	if err := utils.ValidateKey(rootKey, emptyStorageKey); err != nil {
		return err
	}

	if !ds.lockKey(rootKey) {
		return errors.ErrStorageAccessConflict
	}
	defer ds.unLockKey(rootKey)

	absKey := filepath.Join(ds.baseDir, rootKey.Key())
	info, err := os.Stat(absKey)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	} else if info.Mode().IsRegular() {
		return os.Remove(absKey)
	} else if info.IsDir() {
		return os.RemoveAll(absKey)
	}

	return fmt.Errorf("%s is exist, but not recognized, %v", rootKey, info.Mode())
}

// Recover recover storage error
func (ds *diskStorage) Recover(key interfaces.Key) error {
	if !ds.lockKey(key) {
		return nil
	}
	defer ds.unLockKey(key)

	dir := filepath.Join(ds.baseDir, key.Key())
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Mode().IsRegular() {
			if isTmpFile(path) {
				tmpKey := strings.TrimPrefix(path, ds.baseDir)
				key := getKey(tmpKey)
				keyPath := filepath.Join(ds.baseDir, key)
				iErr := os.Rename(path, keyPath)
				if iErr != nil {
					klog.V(2).Infof("failed to recover bytes %s, %v", tmpKey, err)
					return nil
				}
				klog.V(2).Infof("bytes %s recovered successfully", key)
			}
		}

		return nil
	})

	return err
}

func (ds *diskStorage) lockKey(key interfaces.Key) bool {
	keyStr := key.Key()
	ds.Lock()
	defer ds.Unlock()
	if _, ok := ds.keyPendingStatus[keyStr]; ok {
		klog.Infof("key(%s) storage is pending, just skip it", keyStr)
		return false
	}

	for pendingKey := range ds.keyPendingStatus {
		if len(keyStr) > len(pendingKey) {
			if strings.Contains(keyStr, fmt.Sprintf("%s/", pendingKey)) {
				klog.Infof("key(%s) storage is pending, skip to store key(%s)", pendingKey, keyStr)
				return false
			}
		} else {
			if strings.Contains(pendingKey, fmt.Sprintf("%s/", keyStr)) {
				klog.Infof("key(%s) storage is pending, skip to store key(%s)", pendingKey, keyStr)
				return false
			}
		}
	}
	ds.keyPendingStatus[keyStr] = struct{}{}
	return true
}

func (ds *diskStorage) canUpdateList(key interfaces.Key, selector string) (bool, error) {
	keyStr := key.Key()
	ds.Lock()
	defer ds.Unlock()
	if oldSelector, ok := ds.listSelectorCollector[keyStr]; ok {
		if oldSelector != selector {
			// list requests that have the same path but with different selector, for example:
			// request1: http://{ip:port}/api/v1/default/pods?labelSelector=foo=bar
			// request2: http://{ip:port}/api/v1/default/pods?labelSelector=foo2=bar2
			// because func queryListObject() will get all pods for both requests instead of
			// getting pods by request selector. so cache manager can not support same path list
			// requests that has different selector.
			klog.Warningf("list requests that have the same path but with different selector, skip cache for %s", keyStr)
			return false, fmt.Errorf("selector conflict, old selector is %s, current selector is %s", oldSelector, selector)
		}
	} else {
		// list requests that get the same resources but with different path, for example:
		// request1: http://{ip/port}/api/v1/pods?fieldSelector=spec.nodeName=foo
		// request2: http://{ip/port}/api/v1/default/pods?fieldSelector=spec.nodeName=foo
		// because func queryListObject() will get all pods for both requests instead of
		// getting pods by request selector. so cache manager can not support getting same resource
		// list requests that has different path.
		for k := range ds.listSelectorCollector {
			if (len(k) > len(keyStr) && strings.Contains(k, keyStr)) || (len(k) < len(keyStr) && strings.Contains(keyStr, k)) {
				klog.Warningf("list requests that get the same resources but with different path, skip cache for %s", key)
				return false, fmt.Errorf("path conflict, old path is %s, current path is %s", k, keyStr)
			}
		}
		ds.listSelectorCollector[keyStr] = selector
	}
	return true, nil
}

func (ds *diskStorage) unLockKey(key interfaces.Key) {
	ds.Lock()
	defer ds.Unlock()
	delete(ds.keyPendingStatus, key.Key())
}

func getTmpKey(key interfaces.Key) storageKey {
	dir, file := filepath.Split(key.Key())
	return storageKey(filepath.Join(dir, fmt.Sprintf("%s%s", tmpPrefix, file)))
}

func isTmpFile(path string) bool {
	_, file := filepath.Split(path)
	return strings.HasPrefix(file, tmpPrefix)
}

func getKey(tmpKey string) string {
	dir, file := filepath.Split(tmpKey)
	return filepath.Join(dir, strings.TrimPrefix(file, tmpPrefix))
}

func ObjectResourceVersion(obj runtime.Object) (uint64, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	version := accessor.GetResourceVersion()
	if len(version) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(version, 10, 64)
}

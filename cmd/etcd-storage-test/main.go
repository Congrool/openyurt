package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/poolspirit"
	"k8s.io/klog/v2"
)

var store storage.Store
var kv map[string]string
var prefix string = "/test"

func init() {
	klog.InitFlags(flag.CommandLine)
}

func main() {
	var endpoint, keyFile, certFile, caFile string
	flag.StringVar(&endpoint, "endpoint", "https://127.0.0.1:2379", "etcd endpoint.")
	flag.StringVar(&keyFile, "key", "", "TLS client key.")
	flag.StringVar(&certFile, "cert", "", "TLS client certificate.")
	flag.StringVar(&caFile, "cacert", "", "Server TLS CA certificate.")
	flag.Parse()
	klog.Infof("start to create poolspirit storage")
	s, err := poolspirit.NewStorage(context.TODO(), prefix, endpoint, certFile, keyFile, caFile)
	if err != nil {
		panic(fmt.Errorf("failed to create storage, %v", err))
	}
	store = s

	kv = make(map[string]string)
	// init key
	for i := 0; i < 10; i++ {
		tmp := fmt.Sprintf("%s-%d", "sub", i)
		kv[tmp] = tmp
	}

	klog.Info("run create_test")
	create_test()
	klog.Info("run get_test")
	get_test()
	klog.Info("run update_test")
	update_test()
	delete_test()
	list_test()
	listKeys_test()
	deleteCollection_test()
	replace_test()
}

func create_test() {
	if err := store.Create("first", []byte("first")); err != nil {
		klog.Fatalf("failed to create first, err: %v", err)
	}

	if err := store.Create("first", []byte("first")); err != nil {
		if err != storage.ErrKeyExists {
			klog.Fatalf("create key that exists, unexpected err: %v", err)
		}
	}

	// create serval keys for further test
	for k, v := range kv {
		_ = store.Create(k, []byte(v))
	}
}

func get_test() {
	for k, v := range kv {
		val, err := store.Get(k)
		if err != nil && string(v) != v {
			klog.Fatal("failed on get first, err: %s, val: %s, expected: first", err, string(val))
		}
	}
}

func update_test() {
	for k, v := range kv {
		newV := []byte(fmt.Sprintf("%s-update", string(v)))
		_, err := store.Update(k, newV, 99999999999, false)
		if err != nil {
			klog.Fatalf("failed to update key: %s, %v", k, err)
		}

		_, err = store.Update(k, []byte(v), 0, false)
		if err == nil || err != storage.ErrUpdateConflict {
			klog.Fatalf("failed on update with an old key, get err: %v", err)
		}

		_, err = store.Update(k, []byte(v), 0, true)
		if err != nil {
			klog.Fatalf("failed to update by force, %v", err)
		}
		break
	}
}

func delete_test() {
	if err := store.Delete("non-exist"); err != nil && err != storage.ErrStorageNotFound {
		klog.Fatalf("failed at delete_test when deleting key that not exists, %v", err)
	}

	for k, v := range kv {
		if err := store.Delete(k); err != nil {
			klog.Fatalf("failed at delete_test when deleting key-value %s: %s, %v", k, v, err)
		}
		delete(kv, k)
		break
	}
}

func listKeys_test() {
	newPrefix := "new"
	_, err := store.ListKeys(newPrefix)
	if err != nil {
		klog.Fatalf("failed at listKeys_test, %v", err)
	}

}

func list_test() {
	newPrefix := "new"
	for k, v := range kv {
		_ = store.Create(filepath.Join(newPrefix, k), []byte(v))
	}

	kvs, err := store.List(newPrefix)
	if err != nil {
		klog.Fatalf("failed at list_test, %v", err)
	}

	for _, v := range kvs {
		found := false
		for _, mv := range kv {
			if string(v) == mv {
				found = true
				break
			}
		}
		if !found {
			klog.Fatalf("failed at list_test, found value that should not exist, %s", v)
		}
	}

	for _, mv := range kv {
		found := false
		for _, v := range kvs {
			if string(v) == mv {
				found = true
				break
			}
		}

		if !found {
			klog.Fatalf("failed at list_test, %s should exist but not found", mv)
		}
	}
}

func deleteCollection_test() {
	if err := store.DeleteCollection("modified"); err != nil {
		klog.Fatalf("failed at deleteCollection_test, %v", err)
	}

	if err := store.DeleteCollection("new"); err != nil {
		klog.Fatalf("failed at deleteCollection_test, %v", err)
	}

	now, _ := store.ListKeys("new")
	if len(now) != 0 {
		klog.Fatalf("failed at deleteCollection_test, keys with prefix of %s should all be deleted, but get %d left", prefix, len(now))
	}
}

func replace_test() {
	copy := map[string][]byte{}
	rvs := map[string]int64{}
	for k, v := range kv {
		k = filepath.Join("new", k)
		copy[k] = []byte(v)
		rvs[k] = 9999999999
	}

	newPrefix := "new"
	if err := store.Replace(newPrefix, copy, rvs); err != nil {
		klog.Fatalf("failed at replace_test, %v", err)
	}

	now, _ := store.ListKeys(newPrefix)
	if len(now) != len(copy) {
		klog.Fatalf("failed at replace_test, keys should all be created")
	}

	for k, v := range copy {
		copy[k] = []byte(string(v) + "modified")
	}
	if err := store.Replace(newPrefix, copy, rvs); err != nil {
		klog.Fatalf("failed at replace_test, %v", err)
	}
	nowV, _ := store.List(prefix)
	for _, v := range nowV {
		if !strings.HasSuffix(string(v), "modified") {
			klog.Fatalf("failed at replace_test, the value is not replaced")
		}
	}
}

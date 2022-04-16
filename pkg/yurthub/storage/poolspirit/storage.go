package poolspirit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/clientv3"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

const (
	defaultDialTimeout = 10 * time.Second
)

type Storage struct {
	ctx    context.Context
	prefix string
	client *clientv3.Client
}

func NewStorage(ctx context.Context, prefix, serverAddress, certFile, keyFile, caFile string) (storage.Store, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      certFile,
		KeyFile:       keyFile,
		TrustedCAFile: caFile,
	}

	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create tls config for etcd client, %s", err)
	}

	clientConfig := clientv3.Config{
		Endpoints:   []string{serverAddress},
		TLS:         tlsConfig,
		DialTimeout: defaultDialTimeout,
	}

	client, err := clientv3.New(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client, %s", err)
	}

	go func() {
		// shutdown
		<-ctx.Done()
		if err := client.Close(); err != nil {
			klog.Errorf("failed to close the connection to etcd, %s", err)
		}
	}()

	return &Storage{
		ctx:    ctx,
		prefix: prefix,
		client: client,
	}, nil
}

func (s *Storage) Create(key string, content []byte) error {
	klog.V(4).Infof("try to create key %s in pool-cache, content: %s", key, string(content))
	if err := validateKV(key, content); err != nil {
		return err
	}

	txnResp, err := s.client.KV.Txn(s.ctx).If(
		// check if this key exists
		clientv3.Compare(clientv3.ModRevision(key), "=", 0),
	).Then(
		// key does not exist, create it
		clientv3.OpPut(key, string(content)),
	).Commit()

	if err != nil {
		return err
	}

	if !txnResp.Succeeded {
		return storage.ErrKeyExists
	}
	klog.V(4).Infof("%s has been created in pool-cache", key)
	return nil
}

// TODO:
// do not use uint64
// or check how does kubernetes use uint64
func (s *Storage) Update(key string, content []byte, rv uint64, force bool) ([]byte, error) {
	klog.V(4).Infof("try to update key %s in pool-cache", key)
	if err := validateKV(key, content); err != nil {
		return nil, err
	}

	txnResp, err := s.client.KV.Txn(s.ctx).If(
		// check if the key exists and if its version is older
		// TODO:
		// it is not recommended to use ineqality for resource version
		// find a better way to check whether this obj is new or old
		clientv3.Compare(clientv3.ModRevision(key), "<", int64(rv)),
		clientv3.Compare(clientv3.ModRevision(key), ">", 0),
	).Then(
		// update it
		clientv3.OpPut(key, string(content)),
	).Else(
		// Possibly two cases here:
		// 1. key do not exist
		// 2. key exists with a higher rv
		// We can distinguish them by OpGet. If it gets no value back, it's case 1.
		// Otherwise is case 2.
		clientv3.OpGet(key),
	).Commit()

	if err != nil {
		return nil, err
	}

	if !txnResp.Succeeded {
		getResp := (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
		if len(getResp.Kvs) == 0 {
			return nil, storage.ErrStorageNotFound
		}
		if force {
			klog.V(4).Infof("data in etcd has higher rv, update %s by force", key)
			_, err := s.client.Put(s.ctx, key, string(content))
			if err == nil {
				klog.V(4).Infof("failed to update %s, %v", key, err)
				return nil, err
			}
			return nil, nil
		}
		return getResp.Kvs[0].Value, storage.ErrUpdateConflict
	}

	klog.V(4).Infof("succeed to update key %s in pool-cache with rv %d", key, rv)
	return nil, nil
}

func (s *Storage) Delete(key string) error {
	klog.V(4).Infof("try to delete key %s in pool-cache", key)
	if key == "" {
		return storage.ErrKeyIsEmpty
	}

	txnResp, err := s.client.Txn(s.ctx).If(
		// check if the key exists
		clientv3.Compare(clientv3.ModRevision(key), ">", 0),
	).Then(
		// exist, delete this key
		clientv3.OpDelete(key),
	).Commit()

	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return storage.ErrStorageNotFound
	}

	klog.V(4).Infof("succeed to delete key %s in pool-cache", key)
	return nil
}

func (s *Storage) Get(key string) ([]byte, error) {
	klog.V(2).Infof("try to get key %s in pool-cache", key)
	if key == "" {
		return nil, storage.ErrKeyIsEmpty
	}

	getResp, err := s.client.Get(s.ctx, key)
	if err != nil {
		return nil, err
	}
	if len(getResp.Kvs) == 0 {
		return nil, storage.ErrStorageNotFound
	}

	return getResp.Kvs[0].Value, nil
}

func (s *Storage) ListKeys(key string) ([]string, error) {
	klog.V(4).Infof("try to list keys under root key: %s from pool-cache", key)
	if key == "" {
		return nil, storage.ErrKeyIsEmpty
	}

	key = strings.TrimSuffix(key, "/") + "/"
	getResp, err := s.client.Get(s.ctx, key, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}

	if len(getResp.Kvs) == 0 {
		klog.V(4).Infof("no keys found with prefix %s", key)
		return nil, storage.ErrStorageNotFound
	}

	keys := make([]string, 0, len(getResp.Kvs))
	for _, kv := range getResp.Kvs {
		keys = append(keys, string(kv.Key))
	}

	klog.V(4).Infof("succeed to list keys with root key %s from pool-cache", key)
	return keys, nil
}

func (s *Storage) List(key string) ([][]byte, error) {
	klog.V(4).Infof("try to list objs from pool-cache under key: %s", key)
	if key == "" {
		return nil, storage.ErrKeyIsEmpty
	}

	getResp, err := s.client.Get(s.ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if len(getResp.Kvs) == 0 {
		return nil, storage.ErrStorageNotFound
	}

	values := make([][]byte, 0, len(getResp.Kvs))
	for _, kv := range getResp.Kvs {
		values = append(values, kv.Value)
	}
	klog.V(4).Infof("succeed to list objs under root key: %s from pool-cache", key)
	return values, nil
}

func (s *Storage) UpdateList(rootKey string, contents map[string][]byte, rvs map[string]int64, selector string) error {
	klog.V(4).Infof("try to update objs under rootKey: %s in pool-cache", rootKey)
	if rootKey == "" {
		return storage.ErrKeyIsEmpty
	}

	rootKey = strings.TrimSuffix(rootKey, "/") + "/"
	for key := range contents {
		if !strings.Contains(key, rootKey) {
			return storage.ErrRootKeyInvalid
		}
	}

	ops := []clientv3.Op{}

	for k, v := range contents {
		rv := rvs[k]
		existCmp := clientv3.Compare(clientv3.ModRevision(k), "=", 0)
		putOp := clientv3.OpPut(k, string(v))
		rvCmp := clientv3.Compare(clientv3.ModRevision(k), "<", rv)
		updateOp := clientv3.OpTxn([]clientv3.Cmp{rvCmp}, []clientv3.Op{putOp}, nil)
		txnOp := clientv3.OpTxn(
			// check if key exists
			[]clientv3.Cmp{existCmp},
			// if key not exists, then
			[]clientv3.Op{putOp},
			// else
			[]clientv3.Op{updateOp},
		)
		ops = append(ops, txnOp)
	}

	// packet all txn ops into one txn
	_, err := s.client.Txn(s.ctx).If().Then(ops...).Commit()

	if err != nil {
		return err
	}

	klog.V(4).Infof("succeed to update list under root key: %s in pool-cache", rootKey)
	return nil
}

func (s *Storage) DeleteCollection(rootKey string) error {
	klog.V(4).Infof("try to delete collection of rootKey: %s in pool-cache", rootKey)
	if rootKey == "" {
		return storage.ErrKeyIsEmpty
	}

	rootKey = strings.TrimSuffix(rootKey, "/") + "/"

	ops := []clientv3.Op{}
	ops = append(ops,
		// delete keys with prefix of rootKey
		clientv3.OpDelete(rootKey, clientv3.WithPrefix()),
		// delete rootKey itself
		clientv3.OpDelete(strings.TrimSuffix(rootKey, "/")),
	)

	txnResp, err := s.client.Txn(s.ctx).If(
		// if keys with prefix of rootKey do not exist
		clientv3.Compare(clientv3.ModRevision(rootKey).WithPrefix(), "=", 0),
		// if rootKey itself does not exist
		clientv3.Compare(clientv3.ModRevision(strings.TrimSuffix(rootKey, "/")), "=", 0),
	).Then(
	// do nothing
	).Else(
		// delete keys with the prefix of rootkey and the rootkey itself
		ops...,
	).Commit()

	if err != nil {
		return nil
	}

	if !txnResp.Succeeded {
		klog.V(4).Infof("keys with prefix of %s do not exist, skip deleting them in pool-cache", rootKey)
	}

	klog.V(4).Infof("succeed to delete objs under root key: %s in pool-cache", rootKey)
	return nil
}

func validateKV(key string, content []byte) error {
	if key == "" {
		return storage.ErrKeyIsEmpty
	} else if len(content) == 0 {
		return storage.ErrKeyHasNoContent
	}

	return nil
}

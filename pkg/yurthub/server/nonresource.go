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

package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/cmd/yurthub/app/config"
	"github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

var nonResourceReqPaths = map[string]storage.ClusterInfoType{
	"/version":                       storage.Version,
	"/apis/discovery.k8s.io/v1":      storage.APIResourcesInfo,
	"/apis/discovery.k8s.io/v1beta1": storage.APIResourcesInfo,
}

type handleNonResourceRequestWrapper struct {
	client  *kubernetes.Clientset
	storage cachemanager.StorageWrapper
}

func (h *handleNonResourceRequestWrapper) Wrap(handler http.Handler) http.Handler {
	wrapMux := mux.NewRouter()
	for path := range nonResourceReqPaths {
		wrapMux.HandleFunc(path, h.cacheNonResourceInfo).Methods("GET")
	}
	wrapMux.PathPrefix("/").Handler(handler)
	return wrapMux
}

func (h *handleNonResourceRequestWrapper) cacheNonResourceInfo(w http.ResponseWriter, req *http.Request) {
	copyHeader(w.Header(), req.Header)
	if _, ok := nonResourceReqPaths[req.URL.Path]; !ok {
		ErrNonResource(fmt.Errorf("non-resource request %s cannot be recognized", req.URL.Path), w, req)
		return
	}
	clusterInfoKey := storage.ClusterInfoKey{
		ClusterInfoType: nonResourceReqPaths[req.URL.Path],
		UrlPath:         req.URL.Path,
	}
	// query server
	nonResourceInfo, err := h.client.RESTClient().Get().AbsPath(req.URL.Path).Do(context.TODO()).Raw()
	if err == nil {
		if _, err = w.Write(nonResourceInfo); err != nil {
			klog.Errorf("failed to write the non-resource info, the error is: %v", err)
			ErrNonResource(err, w, req)
			return
		}

		klog.Infof("success to query the cache non-resource info: %s", clusterInfoKey.UrlPath)
		if err = h.storage.SaveClusterInfo(clusterInfoKey, nonResourceInfo); err != nil {
			klog.Errorf("failed to save cluster info in cache for %s, %v", req.URL.Path, err)
		}
	} else {
		// query cache
		klog.Errorf("failed to query server for request %s, %v, try to use cache", req.URL.Path, err)
		nonResourceInfo, err = h.storage.GetClusterInfo(clusterInfoKey)
		if err != nil {
			klog.Errorf("the non-resource info cannot be acquired, the error is: %v", err)
			ErrNonResource(err, w, req)
			return
		}
		_, err = w.Write(nonResourceInfo)
		if err != nil {
			klog.Errorf("failed to write the non-resource info, the error is: %v", err)
			return
		}
	}
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		if k == "Content-Type" || k == "Content-Length" {
			for _, v := range vv {
				dst.Add(k, v)
			}
		}
	}
}

func ErrNonResource(err error, w http.ResponseWriter, req *http.Request) {
	status := responsewriters.ErrorToAPIStatus(err)
	code := int(status.Code)
	// when writing an error, check to see if the status indicates a retry after period
	if status.Details != nil && status.Details.RetryAfterSeconds > 0 {
		delay := strconv.Itoa(int(status.Details.RetryAfterSeconds))
		w.Header().Set("Retry-After", delay)
	}

	if code == http.StatusNoContent {
		w.WriteHeader(code)
	}
	klog.Errorf("%v counter the error %v", req.URL, err)

}

func NewHandleNonResourceRequestWrapper(cfg *config.YurtHubConfiguration, client *kubernetes.Clientset) *handleNonResourceRequestWrapper {
	return &handleNonResourceRequestWrapper{
		client:  client,
		storage: cfg.StorageWrapper,
	}
}

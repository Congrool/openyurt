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

package local

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	manager "github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog"
)

const (
	interval = 2 * time.Second
)

// IsHealthy is func for fetching healthy status of remote server
type IsHealthy func() bool

// LocalProxy is responsible for handling requests when remote servers are unhealthy
type LocalProxy struct {
	cacheMgr manager.CacheManager
	// isHealthy is not the status of local proxy
	isHealthy IsHealthy
}

// NewLocalProxy creates a *LocalProxy
func NewLocalProxy(cacheMgr manager.CacheManager, isHealthy IsHealthy) *LocalProxy {
	return &LocalProxy{
		cacheMgr:  cacheMgr,
		isHealthy: isHealthy,
	}
}

// ServeHTTP implements http.Handler for LocalProxy
func (lp *LocalProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var err error
	ctx := req.Context()
	if reqInfo, ok := apirequest.RequestInfoFrom(ctx); ok && reqInfo != nil && reqInfo.IsResourceRequest {
		klog.V(3).Infof("go into local proxy for request %s", util.ReqString(req))
		switch reqInfo.Verb {
		case "watch":
			err = lp.localWatch(w, req)
		case "create":
			err = lp.localPost(w, req)
		case "delete", "deletecollection":
			err = localDelete(w, req)
		default: // list., get, update
			err = lp.localReqCache(w, req)
		}

		if err != nil {
			klog.Errorf("could not proxy local for %s, %v", util.ReqString(req), err)
			util.Err(err, w, req)
		}
	} else {
		klog.Errorf("request(%s) is not supported when cluster is unhealthy", util.ReqString(req))
		util.Err(errors.NewBadRequest(fmt.Sprintf("request(%s) is not supported when cluster is unhealthy", util.ReqString(req))), w, req)
	}
}

// localDelete handles Delete requests when remote servers are unhealthy
func localDelete(w http.ResponseWriter, req *http.Request) error {
	ctx := req.Context()
	info, _ := apirequest.RequestInfoFrom(ctx)
	s := &metav1.Status{
		Status: metav1.StatusSuccess,
		Code:   http.StatusOK,
		Reason: metav1.StatusReasonForbidden,
		Details: &metav1.StatusDetails{
			Name:  info.Name,
			Group: info.Namespace,
			Kind:  info.Resource,
		},
		Message: "delete request is not supported in local cache",
	}

	util.WriteObject(http.StatusOK, s, w, req)
	return nil
}

// localPost handles Create requests when remote servers are unhealthy
func (lp *LocalProxy) localPost(w http.ResponseWriter, req *http.Request) error {
	var buf bytes.Buffer

	ctx := req.Context()
	info, _ := apirequest.RequestInfoFrom(ctx)
	if info.Resource == "events" {
		stopCh := make(chan struct{})
		// rc is new created dualReadCloser
		// prc is pipe readcloser.
		rc, prc := util.NewDualReadCloser(req.Body, false)
		go func(ctx context.Context, prc io.ReadCloser, stopCh <-chan struct{}) {
			klog.V(2).Infof("cache events when cluster is unhealthy, %v", lp.cacheMgr.CacheResponse(ctx, prc, stopCh))
		}(ctx, prc, stopCh)

		req.Body = rc
	}

	headerNStr := req.Header.Get("Content-Length")
	headerN, _ := strconv.Atoi(headerNStr)

	// ReadFrom req.Body will call dualReadCloser.Read(), that means we can read the req.body from prc as well.
	n, err := buf.ReadFrom(req.Body)
	if err != nil || (headerN != 0 && int(n) != headerN) {
		// Is it possible for HTTP?
		klog.Warningf("read body of post request when cluster is unhealthy, expect %d bytes but get %d bytes with error, %v", headerN, n, err)
	}

	// close the pipe only, request body will be closed by http request caller
	if info.Resource == "events" {
		req.Body.Close()
	}

	copyHeader(w.Header(), req.Header)
	w.WriteHeader(http.StatusCreated)

	nw, err := w.Write(buf.Bytes())
	if err != nil || nw != int(n) {
		klog.Errorf("write resp for post request when cluster is unhealthy, expect %d bytes but write %d bytes with error, %v", n, nw, err)
	}
	klog.V(5).Infof("post request %s when cluster is unhealthy", buf.String())

	return nil
}

// localWatch handles Watch requests when remote servers are unhealthy
func (lp *LocalProxy) localWatch(w http.ResponseWriter, req *http.Request) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		err := fmt.Errorf("unable to start watch - can't get http.Flusher: %#v", w)
		return errors.NewInternalError(err)
	}

	opts := metainternalversion.ListOptions{}
	if err := metainternalversion.ParameterCodec.DecodeParameters(req.URL.Query(), metav1.SchemeGroupVersion, &opts); err != nil {
		return errors.NewBadRequest(err.Error())
	}

	ctx := req.Context()
	contentType, _ := util.ReqContentTypeFrom(ctx)
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	timeout := time.Duration(0)
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	} else {
		return nil
	}

	watchTimer := time.NewTimer(timeout)
	// Why set intervalTicker here?
	intervalTicker := time.NewTicker(interval)
	defer watchTimer.Stop()
	defer intervalTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.Infof("exit request %s for context: %v", util.ReqString(req), ctx.Err())
			return nil
		case <-watchTimer.C:
			return nil
		case <-intervalTicker.C:
			// if cluster becomes healthy, exit the watch wait
			if lp.isHealthy() {
				return nil
			}
		}
	}
}

// localReqCache handles Get/List/Update requests when remote servers are unhealthy
func (lp *LocalProxy) localReqCache(w http.ResponseWriter, req *http.Request) error {
	if !lp.cacheMgr.CanCacheFor(req) {
		klog.Errorf("can not cache for %s", util.ReqString(req))
		return errors.NewBadRequest(fmt.Sprintf("can not cache for %s", util.ReqString(req)))
	}

	obj, err := lp.cacheMgr.QueryCache(req)
	if err == storage.ErrStorageNotFound {
		reqInfo, _ := apirequest.RequestInfoFrom(req.Context())
		return errors.NewNotFound(schema.GroupResource{Group: reqInfo.APIGroup, Resource: reqInfo.Resource}, reqInfo.Name)
	} else if err != nil {
		klog.Errorf("failed to query cache for %s, %v", util.ReqString(req), err)
		return errors.NewInternalError(err)
	} else if obj == nil {
		klog.Errorf("no cache object for %s", util.ReqString(req))
		return errors.NewInternalError(fmt.Errorf("no cache object for %s", util.ReqString(req)))
	}

	util.WriteObject(http.StatusOK, obj, w, req)
	return nil
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

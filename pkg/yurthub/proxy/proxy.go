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

package proxy

import (
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/cmd/yurthub/app/config"
	"github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate/interfaces"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy/local"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy/pool"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy/remote"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/transport"
	hubutil "github.com/openyurtio/openyurt/pkg/yurthub/util"
)

type yurtReverseProxy struct {
	resolver             apirequest.RequestInfoResolver
	loadBalancer         Backend
	checker              healthchecker.HealthChecker
	localProxy           *local.LocalProxy
	poolCoordinatorProxy *pool.PoolCoordinatorProxy
	cacheMgr             cachemanager.CacheManager
	maxRequestsInFlight  int
	stopCh               <-chan struct{}
}

// Backend is an interface for proxying http request to a backend handler.
type Backend interface {
	IsHealthy() bool
	ServeHTTP(rw http.ResponseWriter, req *http.Request)
}

// NewYurtReverseProxyHandler creates a http handler for proxying
// all of incoming requests.
func NewYurtReverseProxyHandler(
	yurtHubCfg *config.YurtHubConfiguration,
	cacheMgr cachemanager.CacheManager,
	transportMgr transport.Interface,
	healthChecker healthchecker.HealthChecker,
	certManager interfaces.YurtCertificateManager,
	stopCh <-chan struct{}) (http.Handler, error) {
	cfg := &server.Config{
		LegacyAPIGroupPrefixes: sets.NewString(server.DefaultLegacyAPIPrefix),
	}
	resolver := server.NewRequestInfoResolver(cfg)

	lb, err := remote.NewLoadBalancer(
		yurtHubCfg.LBMode,
		yurtHubCfg.RemoteServers,
		cacheMgr,
		transportMgr,
		healthChecker,
		certManager,
		yurtHubCfg.FilterChain,
		stopCh)
	if err != nil {
		return nil, err
	}

	// var localProxy *local.LocalProxy
	// // When yurthub is working in cloud mode, cacheMgr will be set to nil which means the local cache is disabled,
	// // so we don't need to create a LocalProxy.
	// if cacheMgr != nil {
	// 	localProxy = local.NewLocalProxy(cacheMgr, lb.IsHealthy)
	// }

	// TODO: ReverseProxyHandler should know which cacheMgr to use
	var poolCoordinatorProxy *pool.PoolCoordinatorProxy
	if cacheMgr != nil {
		proxy, err := pool.NewPoolCoordinatorProxy(
			yurtHubCfg.PoolSpiritServerAddr,
			cacheMgr,
			transportMgr,
			healthChecker,
			certManager,
			stopCh)
		if err != nil {
			return nil, fmt.Errorf("failed to create pool-coordinator proxy, %v", err)
		}
		poolCoordinatorProxy = proxy
	}

	yurtProxy := &yurtReverseProxy{
		resolver:             resolver,
		loadBalancer:         lb,
		checker:              healthChecker,
		localProxy:           nil,
		poolCoordinatorProxy: poolCoordinatorProxy,
		cacheMgr:             cacheMgr,
		maxRequestsInFlight:  yurtHubCfg.MaxRequestInFlight,
		stopCh:               stopCh,
	}

	return yurtProxy.buildHandlerChain(yurtProxy), nil
}

func (p *yurtReverseProxy) buildHandlerChain(handler http.Handler) http.Handler {
	handler = util.WithRequestTrace(handler)
	handler = util.WithRequestContentType(handler)
	if p.cacheMgr != nil {
		handler = util.WithCacheHeaderCheck(handler)
	}
	handler = util.WithRequestTimeout(handler)
	if p.cacheMgr != nil {
		handler = util.WithListRequestSelector(handler)
	}
	handler = util.WithMaxInFlightLimit(handler, p.maxRequestsInFlight)
	handler = util.WithRequestClientComponent(handler)
	handler = filters.WithRequestInfo(handler, p.resolver)
	return handler
}

func (p *yurtReverseProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if hubutil.IsKubeletLeaseReq(req) {
		p.handleKubeletLease(rw, req)
		return
	}

	// The priority is LoadBalancer > PoolCoordiantorProxy > LocalProxy.
	// The enqueue order is also the query order.
	handlers := []Backend{p.loadBalancer}
	if p.poolCoordinatorProxy != nil {
		handlers = append(handlers, p.poolCoordinatorProxy)
	}
	if p.localProxy != nil {
		handlers = append(handlers, p.localProxy)
	}

	for _, h := range handlers {
		if h.IsHealthy() {
			h.ServeHTTP(rw, req)
			return
		}
	}

	// no handler is healthy
	err := fmt.Errorf("cannot handle request %s, no handler is healthy", hubutil.ReqString(req))
	klog.Error(err)
	// TODO: set the retry period outward
	hubutil.Err(errors.NewTimeoutError(err.Error(), 60), rw, req)
}

func (p *yurtReverseProxy) handleKubeletLease(rw http.ResponseWriter, req *http.Request) {
	// In edge mode: the health checker will send the kubelet lease
	// In cloud mode: the health checker is fake, and the kubelet lease
	// request should be proxyed
	p.checker.UpdateLastKubeletLeaseReqTime(time.Now())
	if p.poolCoordinatorProxy != nil {
		// TODO: pool-coordinator cannot handle the request currently.
		p.poolCoordinatorProxy.ServeHTTP(rw, req)
	} else if p.localProxy != nil {
		p.localProxy.ServeHTTP(rw, req)
	} else {
		// Only in cloud mode pool-coordiantor proxy and
		// localproxy can both be nil. So we should proxy the
		// kubelet lease request with lb.
		p.loadBalancer.ServeHTTP(rw, req)
	}
}

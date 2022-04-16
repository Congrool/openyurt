package pool

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate/interfaces"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	proxyutil "github.com/openyurtio/openyurt/pkg/yurthub/proxy/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/transport"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"
)

// LocalProxy is responsible for handling requests when remote servers are unhealthy
type PoolCoordinatorProxy struct {
	poolCoordinatorProxy *proxyutil.RemoteProxy
	cacheMgr             cachemanager.CacheManager
}

func NewPoolCoordinatorProxy(
	poorCoordinatorAddr *url.URL,
	cacheMgr cachemanager.CacheManager,
	transportMgr transport.Interface,
	healthChecker healthchecker.HealthChecker,
	certManager interfaces.YurtCertificateManager,
	stopCh <-chan struct{}) (*PoolCoordinatorProxy, error) {
	if poorCoordinatorAddr == nil {
		return nil, fmt.Errorf("pool-coordinator addr cannot be nil")
	}

	// TODO:
	// consider if we need filter chain here
	proxy, err := proxyutil.NewRemoteProxy(poorCoordinatorAddr, cacheMgr, transportMgr, healthChecker, nil, stopCh)
	if err != nil {
		return nil, fmt.Errorf("failed to create remote proxy for pool-coordinator, %v", err)
	}

	return &PoolCoordinatorProxy{
		poolCoordinatorProxy: proxy,
		cacheMgr:             cacheMgr,
	}, nil
}

// ServeHTTP of PoolCoordinatorProxy is able to handle read-only request, including
// watch, list, get. Other verbs that will write data to the cache are not supported
// currently.
func (pp *PoolCoordinatorProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var err error
	ctx := req.Context()
	if reqInfo, ok := apirequest.RequestInfoFrom(ctx); ok && reqInfo != nil && reqInfo.IsResourceRequest {
		switch reqInfo.Verb {
		case "create":
			err = pp.poolPost(rw, req)
		case "patch", "update":
			err = pp.poolQuery(rw, req)
		case "delete", "deletecollection":
			err = notHandle("delete", rw, req)
		case "list", "get":
			pp.poolCoordinatorProxy.ServeHTTP(rw, req)
		default:
			err = fmt.Errorf("unsupported verb for pool coordinator proxy: %s", reqInfo.Verb)
		}
		if err != nil {
			klog.Errorf("could not proxy to pool-coordinator for %s, %v", util.ReqString(req), err)
			util.Err(err, rw, req)
		}
	} else {
		klog.Errorf("pool-coordinator does not support request(%s) when cluster is unhealthy", util.ReqString(req))
		util.Err(errors.NewBadRequest(fmt.Sprintf("pool-coordinator does not support request(%s) when cluster is unhealthy", util.ReqString(req))), rw, req)
	}
}

func (pp *PoolCoordinatorProxy) IsHealthy() bool {
	return pp.poolCoordinatorProxy.IsHealthy()
}

func (pp *PoolCoordinatorProxy) poolPost(rw http.ResponseWriter, req *http.Request) error {
	var buf bytes.Buffer

	ctx := req.Context()
	info, _ := apirequest.RequestInfoFrom(ctx)
	reqContentType, _ := util.ReqContentTypeFrom(ctx)
	if info.Resource == "events" && len(reqContentType) != 0 {
		ctx = util.WithRespContentType(ctx, reqContentType)
		req = req.WithContext(ctx)
		stopCh := make(chan struct{})
		rc, prc := util.NewDualReadCloser(req, req.Body, false)
		go func(req *http.Request, prc io.ReadCloser, stopCh <-chan struct{}) {
			klog.V(2).Infof("cache events when cluster is unhealthy, %v",
				pp.cacheMgr.CacheResponse(req, prc, stopCh))
		}(req, prc, stopCh)

		req.Body = rc
	}

	headerNStr := req.Header.Get("Content-Length")
	headerN, _ := strconv.Atoi(headerNStr)
	n, err := buf.ReadFrom(req.Body)
	if err != nil || (headerN != 0 && int(n) != headerN) {
		klog.Warningf("read body of post request when cluster is unhealthy, expect %d bytes but get %d bytes with error, %v", headerN, n, err)
	}

	// close the pipe only, request body will be closed by http request caller
	if info.Resource == "events" {
		req.Body.Close()
	}

	proxyutil.CopyHeader(rw.Header(), req.Header)
	rw.WriteHeader(http.StatusCreated)

	nw, err := rw.Write(buf.Bytes())
	if err != nil || nw != int(n) {
		klog.Errorf("write resp for post request when cluster is unhealthy, expect %d bytes but write %d bytes with error, %v", n, nw, err)
	}
	klog.V(5).Infof("post request %s when cluster is unhealthy", buf.String())

	return nil
}

// localReqCache handles Get/List/Update requests when remote servers are unhealthy
func (pp *PoolCoordinatorProxy) poolQuery(rw http.ResponseWriter, req *http.Request) error {
	if !pp.cacheMgr.CanCacheFor(req) {
		klog.Errorf("can not cache for %s", util.ReqString(req))
		return errors.NewBadRequest(fmt.Sprintf("can not cache for %s", util.ReqString(req)))
	}

	req.Method = "GET"
	pp.poolCoordinatorProxy.ServeHTTP(rw, req)
	return nil
}

func notHandle(verb string, w http.ResponseWriter, req *http.Request) error {
	ctx := req.Context()
	info, _ := apirequest.RequestInfoFrom(ctx)
	s := &metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusForbidden,
		Reason: metav1.StatusReasonForbidden,
		Details: &metav1.StatusDetails{
			Name:  info.Name,
			Group: info.Namespace,
			Kind:  info.Resource,
		},
		Message: fmt.Sprintf("verb %s is not supported by pool-coordinator proxy", verb),
	}

	util.WriteObject(http.StatusForbidden, s, w, req)
	return nil
}

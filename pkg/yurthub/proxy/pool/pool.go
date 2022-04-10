package pool

import (
	"fmt"
	"net/http"
	"net/url"

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
		case "update", "create", "delete", "deletecollection":
			// TODO:
			// consider if we can support create/update verb like local proxy
			err = notHandle("delete", rw, req)
		case "watch", "list", "get":
			pp.poolCoordinatorProxy.ServeHTTP(rw, req)
		default:
			err = fmt.Errorf("unrecognized verb for pool coordinator proxy: %s", reqInfo.Verb)
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

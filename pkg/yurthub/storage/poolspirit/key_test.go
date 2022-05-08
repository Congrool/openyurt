package poolspirit

import (
	"context"
	"testing"

	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

var s = Storage{
	ctx:    context.Background(),
	prefix: "/registry",
}

var keyFunc = s.GetKeyFunc()
var ctx = context.Background()

func TestKeyFunc(t *testing.T) {
	cases := map[string]struct {
		ctx       context.Context
		namespace string
		name      string
		want      string
	}{
		"core group normal case": {
			ctx: apirequest.WithRequestInfo(ctx, &apirequest.RequestInfo{
				APIGroup:  "",
				Resource:  "pods",
				Namespace: "test",
				Name:      "test-pod",
			}),
			namespace: "",
			name:      "",
			want:      "/registry/pods/test/test-pod",
		},
		"override namespace and name": {
			ctx: apirequest.WithRequestInfo(ctx, &apirequest.RequestInfo{
				APIGroup:  "",
				Resource:  "pods",
				Namespace: "test",
				Name:      "test-pod",
			}),
			namespace: "test-override",
			name:      "test-pod-override",
			want:      "/registry/pods/test-override/test-pod-override",
		},
		"special prefix for node resource": {
			ctx: apirequest.WithRequestInfo(ctx, &apirequest.RequestInfo{
				APIGroup:  "",
				Resource:  "nodes",
				Namespace: "",
				Name:      "test-node",
			}),
			namespace: "",
			name:      "",
			want:      "/registry/minions/test-node",
		},
		"not core group": {
			ctx: apirequest.WithRequestInfo(ctx, &apirequest.RequestInfo{
				APIGroup:  "apps",
				Resource:  "deployments",
				Namespace: "test",
				Name:      "test-deploy",
			}),
			namespace: "",
			name:      "",
			want:      "/registry/deployments/test/test-deploy",
		},
		"special prefix for service resource": {
			ctx: apirequest.WithRequestInfo(ctx, &apirequest.RequestInfo{
				APIGroup:  "networking.k8s.io",
				Resource:  "ingresses",
				Namespace: "test",
				Name:      "test-ingress",
			}),
			namespace: "",
			name:      "",
			want:      "/registry/ingress/test/test-ingress",
		},
	}

	for n, c := range cases {
		key, err := keyFunc(c.ctx, c.namespace, c.name)
		if err != nil {
			t.Errorf("failed get key for case: %s, %v", n, err)
			continue
		}
		if key != c.want {
			t.Errorf("unexpected key for case %s, want: %s, got: %s", n, c.want, key)
		}
	}
}

package syncer

import (
	"k8s.io/client-go/informers"
)

type Syncer struct {
	proxiedInformerFactory informers.SharedInformerFactory
}

func NewSyncer(proxiedInformerFactory informers.SharedInformerFactory) *Syncer {
	return &Syncer{
		proxiedInformerFactory: proxiedInformerFactory,
	}
}

func (s *Syncer) Start() {
	s.syncForRBAC()
}

func (s *Syncer) syncForRBAC() {
	_ = s.proxiedInformerFactory.Rbac().V1().ClusterRoleBindings().Informer()
	_ = s.proxiedInformerFactory.Rbac().V1().ClusterRoles().Informer()
	_ = s.proxiedInformerFactory.Rbac().V1().RoleBindings().Informer()
	_ = s.proxiedInformerFactory.Rbac().V1().Roles().Informer()
	_ = s.proxiedInformerFactory.Core().V1().ServiceAccounts().Informer()
	_ = s.proxiedInformerFactory.Core().V1().Secrets().Informer()
}

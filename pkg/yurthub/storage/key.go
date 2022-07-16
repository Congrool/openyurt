package storage

type Key interface {
	Key() string
	IsRootKey() bool
}

type KeyBuildInfo struct {
	Component string
	Namespace string
	Name      string
	Resources string
}

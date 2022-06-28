package storage

type Key interface {
	Key() string
}

type KeyBuildInfo struct {
	Component string
	Namespace string
	Name      string
	Resources string
}

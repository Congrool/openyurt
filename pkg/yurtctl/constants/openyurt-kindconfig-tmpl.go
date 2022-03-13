package constants

const (
	OpenYurtKindConfig = `apiVersion: kind.x-k8s.io/v1alpha4
kind: Cluster
name: {{.cluster_name}}
nodes:
  - role: control-plane
    image: {{.kind_node_image}}`

	KindWorkerRole = `  - role: worker
    image: {{.kind_node_image}}`
)

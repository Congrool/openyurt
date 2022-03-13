package constants

const (
	OpenYurtClusterfile = `
apiVersion: sealer.cloud/v2
kind: Cluster
metadata:
  name: my-cluster
spec:
  hosts:
  - ips:
    - {{.apiserver_address}}
    roles:
    - master
  image: {{.cluster_image}}
  ssh:
    passwd: {{.passwd}}
    pk: /root/.ssh/id_rsa
    user: root
  env:
  - YurttunnelServerAddress={{.yurttunnel_server_address}}
---
apiVersion: sealer.cloud/v2
kind: KubeadmConfig
metadata:
  name: default-kubernetes-config
spec:
  networking:
    {{if .pod_subnet }}
    podSubnet: {{.pod_subnet}}
    {{end}}
    {{if .service_subnet}}
    serviceSubnet: {{.service_subnet}}
    {{end}}
  controllerManager:
    extraArgs:
      controllers: -nodelifecycle,*,bootstrapsigner,tokencleaner
`
)

/*
Copyright 2022 The OpenYurt Authors.

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

package sealer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"runtime"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/pkg/yurtctl/constants"
	"github.com/openyurtio/openyurt/pkg/yurtctl/util"
	"github.com/openyurtio/openyurt/pkg/yurtctl/util/edgenode"
	strutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/strings"
	tmplutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/templates"
)

const (
	// APIServerAdvertiseAddress flag sets the IP address the API Server will advertise it's listening on. Specify '0.0.0.0' to use the address of the default network interface.
	APIServerAdvertiseAddress = "apiserver-advertise-address"
	//YurttunnelServerAddress flag sets the IP address of Yurttunnel Server.
	YurttunnelServerAddress = "yurt-tunnel-server-address"
	// NetworkingServiceSubnet flag sets the range of IP address for service VIPs.
	NetworkingServiceSubnet = "service-cidr"
	// NetworkingPodSubnet flag sets the range of IP addresses for the pod network. If set, the control plane will automatically allocate CIDRs for every node.
	NetworkingPodSubnet = "pod-network-cidr"
	// OpenYurtVersion flag sets the OpenYurt version for the control plane.
	OpenYurtVersion = "openyurt-version"
	// ImageRepository flag sets the container registry to pull control plane images from.
	ImageRepository = "image-repository"
	// PassWd flag is the password of master server.
	PassWd = "passwd"

	SealerRunCmd = "sealer apply -f %s/Clusterfile"
)

var (
	validSealerVersions = []string{
		"v0.6.1",
	}
)

// NewSealerInitCmd uses tool sealer to initialize a master of OpenYurt cluster.
// It will deploy all openyurt components, such as yurt-app-manager, yurt-tunnel-server, etc.
func NewSealerInitCMD() *cobra.Command {
	o := newSealerOptions()

	cmd := &cobra.Command{
		Use:   "sealer",
		Short: "Setup OpenYurt cluster with sealer",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			initializer := newSealerInitializer(o.Config())
			if err := initializer.Run(); err != nil {
				return err
			}
			return nil
		},
		Args: cobra.NoArgs,
	}

	addFlags(cmd.Flags(), o)
	return cmd
}

type sealerOptions struct {
	AdvertiseAddress        string
	YurttunnelServerAddress string
	ServiceSubnet           string
	PodSubnet               string
	Password                string
	ImageRepository         string
	OpenYurtVersion         string
}

func newSealerOptions() *sealerOptions {
	return &sealerOptions{
		ImageRepository: constants.DefaultOpenYurtImageRegistry,
		OpenYurtVersion: constants.DefaultOpenYurtVersion,
	}
}

func (o *sealerOptions) Validate() error {
	if err := validateServerAddress(o.AdvertiseAddress); err != nil {
		return err
	}
	if o.YurttunnelServerAddress != "" {
		if err := validateServerAddress(o.YurttunnelServerAddress); err != nil {
			return err
		}
	}
	if o.Password == "" {
		return fmt.Errorf("password can't be empty.")
	}

	if o.PodSubnet != "" {
		if err := validateCidrString(o.PodSubnet); err != nil {
			return err
		}
	}
	if o.ServiceSubnet != "" {
		if err := validateCidrString(o.ServiceSubnet); err != nil {
			return err
		}
	}
	return nil
}

func (o *sealerOptions) Config() *sealerConfig {
	return &sealerConfig{
		AdvertiseAddress:        o.AdvertiseAddress,
		YurttunnelServerAddress: o.YurttunnelServerAddress,
		ServiceSubnet:           o.ServiceSubnet,
		PodSubnet:               o.PodSubnet,
		Password:                o.Password,
		ImageRepository:         o.ImageRepository,
		OpenYurtVersion:         o.OpenYurtVersion,
	}
}

type sealerConfig struct {
	AdvertiseAddress        string
	YurttunnelServerAddress string
	ServiceSubnet           string
	PodSubnet               string
	Password                string
	ImageRepository         string
	OpenYurtVersion         string
}

// sealerInitializer init a node to master of openyurt cluster
type sealerInitializer struct {
	sealerConfig
}

func newSealerInitializer(cfg *sealerConfig) *sealerInitializer {
	return &sealerInitializer{
		*cfg,
	}
}

func (ci *sealerInitializer) Run() error {
	if err := checkAndInstallSealer(); err != nil {
		return err
	}

	if err := ci.prepareClusterfile(); err != nil {
		return err
	}

	if err := ci.installCluster(); err != nil {
		return err
	}
	return nil
}

// PrepareClusterfile fill the template and write the Clusterfile to the /tmp
func (ci *sealerInitializer) prepareClusterfile() error {
	klog.Infof("generate Clusterfile for openyurt")
	err := os.MkdirAll(constants.TmpDownloadDir, constants.DirMode)
	if err != nil {
		return err
	}

	clusterfile, err := tmplutil.SubsituteTemplate(constants.OpenYurtClusterfile, map[string]string{
		"apiserver_address":         ci.AdvertiseAddress,
		"cluster_image":             fmt.Sprintf(constants.OpenYurtClusterImageFormat, ci.ImageRepository, ci.OpenYurtVersion),
		"passwd":                    ci.Password,
		"pod_subnet":                ci.PodSubnet,
		"service_subnet":            ci.ServiceSubnet,
		"yurttunnel_server_address": ci.YurttunnelServerAddress,
	})
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(fmt.Sprintf("%s/Clusterfile", constants.TmpDownloadDir), []byte(clusterfile), constants.FileMode)
	if err != nil {
		return err
	}
	return nil
}

func (s *sealerInitializer) installCluster() error {
	klog.Infof("init an openyurt cluster")
	runCmd := fmt.Sprintf(SealerRunCmd, constants.TmpDownloadDir)
	cmd := exec.Command("bash", "-c", runCmd)
	return edgenode.ExecCmd(cmd)
}

func addFlags(flagset *flag.FlagSet, o *sealerOptions) {
	flagset.StringVarP(
		&o.AdvertiseAddress, APIServerAdvertiseAddress, "", o.AdvertiseAddress,
		"The IP address the API Server will advertise it's listening on.",
	)
	flagset.StringVarP(
		&o.YurttunnelServerAddress, YurttunnelServerAddress, "", o.YurttunnelServerAddress,
		"The yurt-tunnel-server address.")
	flagset.StringVarP(
		&o.ServiceSubnet, NetworkingServiceSubnet, "", o.ServiceSubnet,
		"Use alternative range of IP address for service VIPs.",
	)
	flagset.StringVarP(
		&o.PodSubnet, NetworkingPodSubnet, "", o.PodSubnet,
		"Specify range of IP addresses for the pod network. If set, the control plane will automatically allocate CIDRs for every node.",
	)
	flagset.StringVarP(&o.Password, PassWd, "p", o.Password,
		"set master server ssh password",
	)
	flagset.StringVarP(
		&o.OpenYurtVersion, OpenYurtVersion, "", o.OpenYurtVersion,
		`Choose a specific OpenYurt version for the control plane.`,
	)
	flagset.StringVarP(&o.ImageRepository, ImageRepository, "", o.ImageRepository,
		"Choose a registry to pull cluster images from",
	)
}

func validateServerAddress(address string) error {
	ip := net.ParseIP(address)
	if ip == nil {
		return errors.Errorf("cannot parse IP address: %s", address)
	}
	if !ip.IsGlobalUnicast() {
		return errors.Errorf("cannot use %q as the bind address for the API Server", address)
	}
	return nil
}

func validateCidrString(cidr string) error {
	_, _, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil
	}
	return nil
}

// checkAndInstallSealer install sealer, skip install if it exists
func checkAndInstallSealer() error {
	klog.Infof("Check and install sealer")
	sealerExist := false
	if _, err := exec.LookPath("sealer"); err == nil {
		if b, err := exec.Command("sealer", "version").CombinedOutput(); err == nil {
			info := make(map[string]string)
			if err := json.Unmarshal(b, &info); err != nil {
				return fmt.Errorf("Can't get the existing sealer version: %v", err)
			}
			sealerVersion := info["gitVersion"]
			if strutil.IsInStringLst(validSealerVersions, sealerVersion) {
				klog.Infof("Sealer %s already exist, skip install.", sealerVersion)
				sealerExist = true
			} else {
				return fmt.Errorf("The existing sealer version %s is not supported, please clean it. Valid server versions are %v.", sealerVersion, validSealerVersions)
			}
		}
	}

	if !sealerExist {
		// download and install sealer
		packageUrl := fmt.Sprintf(constants.SealerUrlFormat, constants.DefaultSealerVersion, constants.DefaultSealerVersion, runtime.GOARCH)
		savePath := fmt.Sprintf("%s/sealer-%s-linux-%s.tar.gz", constants.TmpDownloadDir, constants.DefaultSealerVersion, runtime.GOARCH)
		klog.V(1).Infof("Download sealer from: %s", packageUrl)
		if err := util.DownloadFile(packageUrl, savePath, 3); err != nil {
			return fmt.Errorf("Download sealer fail: %v", err)
		}
		if err := util.Untar(savePath, constants.TmpDownloadDir); err != nil {
			return err
		}
		comp := "sealer"
		target := fmt.Sprintf("/usr/bin/%s", comp)
		if err := edgenode.CopyFile(constants.TmpDownloadDir+"/"+comp, target, constants.DirMode); err != nil {
			return err
		}
	}
	return nil
}

// Copyright 2019 fatedier, fatedier@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/util/kube"
	"github.com/fatedier/frp/pkg/util/util"
	"github.com/fatedier/frp/pkg/util/vhost"
)

func init() {
	RegisterProxyFactory(reflect.TypeOf(&v1.HTTPSProxyConfig{}), NewHTTPSProxy)
}

type HTTPSProxy struct {
	*BaseProxy
	cfg *v1.HTTPSProxyConfig
}

func NewHTTPSProxy(baseProxy *BaseProxy) Proxy {
	unwrapped, ok := baseProxy.GetConfigurer().(*v1.HTTPSProxyConfig)
	if !ok {
		return nil
	}
	return &HTTPSProxy{
		BaseProxy: baseProxy,
		cfg:       unwrapped,
	}
}

func (pxy *HTTPSProxy) Run() (remoteAddr string, err error) {
	xl := pxy.xl
	routeConfig := &vhost.RouteConfig{}

	defer func() {
		if err != nil {
			pxy.Close()
		}
	}()
	addrs := make([]string, 0)
	for _, domain := range pxy.cfg.CustomDomains {
		if domain == "" {
			continue
		}

		routeConfig.Domain = domain
		l, errRet := pxy.rc.VhostHTTPSMuxer.Listen(pxy.ctx, routeConfig)
		if errRet != nil {
			err = errRet
			return
		}
		xl.Infof("https proxy listen for host [%s]", routeConfig.Domain)
		pxy.listeners = append(pxy.listeners, l)
		addrs = append(addrs, util.CanonicalAddr(routeConfig.Domain, pxy.serverCfg.VhostHTTPSPort))

		if kube.IsKubernetes() {
			xl.Infof("setting custom domain [%s] label for https proxy", routeConfig.Domain)
			// Label the pod with the custom domain for Kubernetes environments
			// This allows Kubernetes to manage the domain routing correctly
			// and ensures that the pod is discoverable via the custom domain.
			// This is particularly useful for Ingress controllers or when using
			// custom DNS solutions in Kubernetes.
			err := kube.LabelPodWithCustomDomain(pxy.ctx, pxy.rc.KubeClient, routeConfig.Domain)
			if err != nil {
				xl.Warnf("failed to label pod with https custom domain [%s]: %v", routeConfig.Domain, err)
			}
		}
	}

	if pxy.cfg.SubDomain != "" {
		routeConfig.Domain = pxy.cfg.SubDomain + "." + pxy.serverCfg.SubDomainHost
		l, errRet := pxy.rc.VhostHTTPSMuxer.Listen(pxy.ctx, routeConfig)
		if errRet != nil {
			err = errRet
			return
		}
		xl.Infof("https proxy listen for host [%s]", routeConfig.Domain)
		pxy.listeners = append(pxy.listeners, l)
		addrs = append(addrs, util.CanonicalAddr(routeConfig.Domain, pxy.serverCfg.VhostHTTPSPort))

		if kube.IsKubernetes() {
			xl.Infof("setting custom domain [%s] label for https subdomain proxy", routeConfig.Domain)

			err := kube.LabelPodWithCustomDomain(pxy.ctx, pxy.rc.KubeClient, routeConfig.Domain)
			if err != nil {
				xl.Warnf("failed to label pod with https subdomain domain [%s]: %v", routeConfig.Domain, err)
			}
		}
	}

	pxy.startCommonTCPListenersHandler()
	remoteAddr = strings.Join(addrs, ",")
	return
}

func (pxy *HTTPSProxy) Close() {
	xl := pxy.xl

	pxy.BaseProxy.Close()

	var activeDomains []string
	var err error

	if pxy.serverCfg.WebServer.Port != 0 && pxy.serverCfg.WebServer.TLS == nil {
		xl.Infof("getting online custom domains for https proxy [%s]", pxy.name)

		url := fmt.Sprintf("http://localhost:%d/api/proxy/https", pxy.serverCfg.WebServer.Port)
		activeDomains, err = getActiveHTTPCustomDomainsExcludingCurrent(url, pxy.name)
		if err != nil {
			xl.Warnf("failed to get online custom domains: %v", err)
			return
		}
	}

	xl.Infof("Active custom domains for https proxy [%s] after stoping current proxy %s", pxy.name, activeDomains)

	for _, domain := range pxy.cfg.CustomDomains {
		if domain == "" {
			continue
		}

		if slices.Contains(activeDomains, domain) {
			xl.Infof("custom domain [%s] is still online, skipping removal", domain)
			continue
		}

		xl.Infof("removing custom domain [%s] label for https proxy", domain)

		if err := kube.RemoveCustomDomainLabelFromPod(pxy.ctx, pxy.rc.KubeClient, domain); err != nil {
			xl.Warnf("failed to remove custom domain label from pod [%s]: %v", pxy.loginMsg.Hostname, err)
		}
	}
}

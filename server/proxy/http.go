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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"slices"
	"strings"

	libio "github.com/fatedier/golib/io"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/util/kube"
	"github.com/fatedier/frp/pkg/util/limit"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/fatedier/frp/pkg/util/util"
	"github.com/fatedier/frp/pkg/util/vhost"
	"github.com/fatedier/frp/server/metrics"
)

func init() {
	RegisterProxyFactory(reflect.TypeOf(&v1.HTTPProxyConfig{}), NewHTTPProxy)
}

type HTTPProxy struct {
	*BaseProxy
	cfg *v1.HTTPProxyConfig

	closeFuncs []func()
}

type ProxyResponseList struct {
	Proxies []ProxyResponse `json:"proxies"`
}

type ProxyResponse struct {
	Name   string               `json:"name"`
	Conf   *ProxyResponseConfig `json:"conf"`
	Status string               `json:"status"`
}

type ProxyResponseConfig struct {
	CustomDomains []string `json:"customDomains"`
}

func NewHTTPProxy(baseProxy *BaseProxy) Proxy {
	unwrapped, ok := baseProxy.GetConfigurer().(*v1.HTTPProxyConfig)
	if !ok {
		return nil
	}
	return &HTTPProxy{
		BaseProxy: baseProxy,
		cfg:       unwrapped,
	}
}

func (pxy *HTTPProxy) Run() (remoteAddr string, err error) {
	xl := pxy.xl
	routeConfig := vhost.RouteConfig{
		RewriteHost:     pxy.cfg.HostHeaderRewrite,
		RouteByHTTPUser: pxy.cfg.RouteByHTTPUser,
		Headers:         pxy.cfg.RequestHeaders.Set,
		ResponseHeaders: pxy.cfg.ResponseHeaders.Set,
		Username:        pxy.cfg.HTTPUser,
		Password:        pxy.cfg.HTTPPassword,
		CreateConnFn:    pxy.GetRealConn,
	}

	locations := pxy.cfg.Locations
	if len(locations) == 0 {
		locations = []string{""}
	}

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
		for _, location := range locations {
			routeConfig.Location = location

			tmpRouteConfig := routeConfig

			// handle group
			if pxy.cfg.LoadBalancer.Group != "" {
				err = pxy.rc.HTTPGroupCtl.Register(pxy.name, pxy.cfg.LoadBalancer.Group, pxy.cfg.LoadBalancer.GroupKey, routeConfig)
				if err != nil {
					return
				}

				pxy.closeFuncs = append(pxy.closeFuncs, func() {
					pxy.rc.HTTPGroupCtl.UnRegister(pxy.name, pxy.cfg.LoadBalancer.Group, tmpRouteConfig)
				})
			} else {
				// no group
				err = pxy.rc.HTTPReverseProxy.Register(routeConfig)
				if err != nil {
					return
				}
				pxy.closeFuncs = append(pxy.closeFuncs, func() {
					pxy.rc.HTTPReverseProxy.UnRegister(tmpRouteConfig)
				})
			}
			addrs = append(addrs, util.CanonicalAddr(routeConfig.Domain, pxy.serverCfg.VhostHTTPPort))

			if kube.IsKubernetes() {
				xl.Infof("Setting custom domain [%s] label for http proxy", routeConfig.Domain)

				err := kube.LabelPodWithCustomDomain(pxy.ctx, pxy.rc.KubeClient, routeConfig.Domain)
				if err != nil {
					xl.Warnf("failed to label pod with http custom domain [%s]: %v", routeConfig.Domain, err)
				}
			}

			xl.Infof("http proxy listen for host [%s] location [%s] group [%s], routeByHTTPUser [%s]",
				routeConfig.Domain, routeConfig.Location, pxy.cfg.LoadBalancer.Group, pxy.cfg.RouteByHTTPUser)
		}
	}

	if pxy.cfg.SubDomain != "" {
		routeConfig.Domain = pxy.cfg.SubDomain + "." + pxy.serverCfg.SubDomainHost
		for _, location := range locations {
			routeConfig.Location = location

			tmpRouteConfig := routeConfig

			// handle group
			if pxy.cfg.LoadBalancer.Group != "" {
				err = pxy.rc.HTTPGroupCtl.Register(pxy.name, pxy.cfg.LoadBalancer.Group, pxy.cfg.LoadBalancer.GroupKey, routeConfig)
				if err != nil {
					return
				}

				pxy.closeFuncs = append(pxy.closeFuncs, func() {
					pxy.rc.HTTPGroupCtl.UnRegister(pxy.name, pxy.cfg.LoadBalancer.Group, tmpRouteConfig)
				})
			} else {
				err = pxy.rc.HTTPReverseProxy.Register(routeConfig)
				if err != nil {
					return
				}
				pxy.closeFuncs = append(pxy.closeFuncs, func() {
					pxy.rc.HTTPReverseProxy.UnRegister(tmpRouteConfig)
				})
			}
			addrs = append(addrs, util.CanonicalAddr(tmpRouteConfig.Domain, pxy.serverCfg.VhostHTTPPort))

			if kube.IsKubernetes() {
				xl.Infof("setting custom domain [%s] label for http proxy", tmpRouteConfig.Domain)

				err := kube.LabelPodWithCustomDomain(pxy.ctx, pxy.rc.KubeClient, tmpRouteConfig.Domain)
				if err != nil {
					xl.Warnf("failed to label pod with http subdomain domain [%s]: %v", tmpRouteConfig.Domain, err)
				}
			}

			xl.Infof("http proxy listen for host [%s] location [%s] group [%s], routeByHTTPUser [%s]",
				routeConfig.Domain, routeConfig.Location, pxy.cfg.LoadBalancer.Group, pxy.cfg.RouteByHTTPUser)
		}
	}
	remoteAddr = strings.Join(addrs, ",")
	return
}

func (pxy *HTTPProxy) GetRealConn(remoteAddr string) (workConn net.Conn, err error) {
	xl := pxy.xl
	rAddr, errRet := net.ResolveTCPAddr("tcp", remoteAddr)
	if errRet != nil {
		xl.Warnf("resolve TCP addr [%s] error: %v", remoteAddr, errRet)
		// we do not return error here since remoteAddr is not necessary for proxies without proxy protocol enabled
	}

	tmpConn, errRet := pxy.GetWorkConnFromPool(rAddr, nil)
	if errRet != nil {
		err = errRet
		return
	}

	var rwc io.ReadWriteCloser = tmpConn
	if pxy.cfg.Transport.UseEncryption {
		rwc, err = libio.WithEncryption(rwc, []byte(pxy.serverCfg.Auth.Token))
		if err != nil {
			xl.Errorf("create encryption stream error: %v", err)
			return
		}
	}
	if pxy.cfg.Transport.UseCompression {
		rwc = libio.WithCompression(rwc)
	}

	if pxy.GetLimiter() != nil {
		rwc = libio.WrapReadWriteCloser(limit.NewReader(rwc, pxy.GetLimiter()), limit.NewWriter(rwc, pxy.GetLimiter()), func() error {
			return rwc.Close()
		})
	}

	workConn = netpkg.WrapReadWriteCloserToConn(rwc, tmpConn)
	workConn = netpkg.WrapStatsConn(workConn, pxy.updateStatsAfterClosedConn)
	metrics.Server.OpenConnection(pxy.GetName(), pxy.GetConfigurer().GetBaseConfig().Type)
	return
}

func (pxy *HTTPProxy) updateStatsAfterClosedConn(totalRead, totalWrite int64) {
	name := pxy.GetName()
	proxyType := pxy.GetConfigurer().GetBaseConfig().Type
	metrics.Server.CloseConnection(name, proxyType)
	metrics.Server.AddTrafficIn(name, proxyType, totalWrite)
	metrics.Server.AddTrafficOut(name, proxyType, totalRead)
}

func (pxy *HTTPProxy) Close() {
	xl := pxy.xl

	pxy.BaseProxy.Close()
	for _, closeFn := range pxy.closeFuncs {
		closeFn()
	}

	var activeDomains []string
	var err error

	if pxy.serverCfg.WebServer.Port != 0 && pxy.serverCfg.WebServer.TLS == nil {
		xl.Infof("getting online custom domains for http proxy [%s]", pxy.name)

		url := fmt.Sprintf("http://localhost:%d/api/proxy/http", pxy.serverCfg.WebServer.Port)
		activeDomains, err = getActiveHTTPCustomDomainsExcludingCurrent(url, pxy.name)
		if err != nil {
			xl.Warnf("failed to get online custom domains: %v", err)
			return
		}
	}

	xl.Infof("Active custom domains for http proxy [%s] after stoping current proxy %s", pxy.name, activeDomains)

	for _, domain := range pxy.cfg.CustomDomains {
		if domain == "" {
			continue
		}

		if slices.Contains(activeDomains, domain) {
			xl.Infof("custom domain [%s] is still online, skipping removal", domain)
			continue
		}

		xl.Infof("removing custom domain [%s] label for http proxy", domain)

		if err := kube.RemoveCustomDomainLabelFromPod(pxy.ctx, pxy.rc.KubeClient, domain); err != nil {
			xl.Warnf("failed to remove custom domain label from pod [%s]: %v", pxy.loginMsg.Hostname, err)
		}
	}
}

func getActiveHTTPCustomDomainsExcludingCurrent(url, proxyName string) ([]string, error) {
	resp, err := http.Get(url)

	if err != nil {
		return nil, fmt.Errorf("failed to get online custom domains: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get online custom domains, status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data ProxyResponseList

	fmt.Printf("Response body: %s\n", string(body))

	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response body: %v", err)
	}

	var domains []string

	for _, proxyResp := range data.Proxies {
		if proxyResp.Name == proxyName {
			continue // Skip the current proxy
		}
		// Ensure all proxies are marked as online
		if proxyResp.Status == "online" && proxyResp.Conf != nil && proxyResp.Conf.CustomDomains != nil {
			domains = append(domains, proxyResp.Conf.CustomDomains...)
		}
	}

	return domains, nil
}

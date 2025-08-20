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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/fatedier/frp/pkg/auth"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/util/kube"
	"github.com/fatedier/frp/pkg/util/util"
	"github.com/fatedier/frp/pkg/util/vhost"
)

func init() {
	RegisterProxyFactory(reflect.TypeOf(&v1.HTTPSProxyConfig{}), NewHTTPSProxy)
}

const (
	customDomainValidationURL = "http://fast-reverse-proxy-api-server-svc.frp.svc/v1/api/validate"
)

type CustomDomainValidationResponse struct {
	IsValid bool `json:"isValid"`
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

func IsIncomingRequestValid(orgId, requestDomain string) (error, bool) {
	params := url.Values{}
	params.Add("customDomain", requestDomain)

	fullURL := customDomainValidationURL + "?" + params.Encode()

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err), false
	}

	req.Header.Add("X-UiPath-Internal-AccountId", orgId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform request: %v", err), false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err), false
	}

	var result CustomDomainValidationResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to unmarshal response body: %v", err), false
	}

	return nil, result.IsValid
}

func GetParitionIDFromToken(token string) (string, error) {
	parts := strings.Split(token, ".")

	if len(parts) != 3 {
		return "", fmt.Errorf("invalid token format")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("failed to decode token payload: %v", err)
	}

	var claims auth.OidcAuthClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("failed to unmarshal token claims: %v", err)
	}

	return claims.PartitionId, nil
}

func (pxy *HTTPSProxy) Run() (remoteAddr string, err error) {
	xl := pxy.xl

	orgId, err := GetParitionIDFromToken(pxy.BaseProxy.GetLoginMsg().PrivilegeKey)
	if err != nil {
		return "", fmt.Errorf("failed to get partition for https proxy [%s]: %v", pxy.name, err)
	}

	xl.Infof("orgId from token %s", orgId)

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

		// err, isvalid := IsIncomingRequestValid(orgId, domain)
		// if err != nil {
		// 	return "", fmt.Errorf("failed to validate custom domain %s for orgId %s: %v", domain, orgId, err)
		// }

		// if !isvalid {
		// 	// Domain is not valid for this orgId
		// 	return "", fmt.Errorf("domain %s is not allowed for orgId %s", domain, orgId)
		// }

		routeConfig.Domain = domain
		l, errRet := pxy.rc.VhostHTTPSMuxer.Listen(pxy.ctx, routeConfig)
		if errRet != nil {
			return "", errRet
		}

		xl.Infof("https proxy listen for host [%s]", routeConfig.Domain)
		pxy.listeners = append(pxy.listeners, l)
		addrs = append(addrs, util.CanonicalAddr(routeConfig.Domain, pxy.serverCfg.VhostHTTPSPort))

		if pxy.rc.KubeClient != nil {
			domainLabel := fmt.Sprintf("%s.%d", routeConfig.Domain, pxy.cfg.LocalPort)
			xl.Infof("setting custom domain [%s] label for https proxy", domainLabel)
			// Label the pod with the custom domain for Kubernetes environments
			// This allows Kubernetes to manage the domain routing correctly
			// and ensures that the pod is discoverable via the custom domain.
			// This is particularly useful for Ingress controllers or when using
			// custom DNS solutions in Kubernetes.
			err := kube.AddRemoveLabelPodWithPrefix(pxy.ctx, pxy.rc.KubeClient, kube.AddLabel, kube.CustomDomainLabelPrefix, domainLabel)
			if err != nil {
				xl.Warnf("failed to label pod with https custom domain [%s]: %v", domainLabel, err)
			}

			err = kube.AddRemoveLabelPodWithPrefix(pxy.ctx, pxy.rc.KubeClient, kube.AddLabel, kube.OrgIdLabelPrefix, orgId)
			if err != nil {
				xl.Warnf("failed to label pod with https custom domain [%s/%s]: %v", kube.OrgIdLabelPrefix, orgId, err)
			}

			pxy.isLabeled = true
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
	}

	pxy.startCommonTCPListenersHandler()
	remoteAddr = strings.Join(addrs, ",")
	return
}

func (pxy *HTTPSProxy) Close() {
	xl := pxy.xl

	pxy.BaseProxy.Close()

	if len(pxy.cfg.CustomDomains) == 0 || pxy.rc.KubeClient == nil || pxy.isLabeled == false {
		return
	}

	for _, domain := range pxy.cfg.CustomDomains {
		if domain == "" {
			continue
		}

		xl.Infof("Removing custom domain [%s] label for https proxy", domain)

		if err := kube.AddRemoveLabelPodWithPrefix(pxy.ctx, pxy.rc.KubeClient, kube.RemoveLabel, kube.CustomDomainLabelPrefix, domain); err != nil {
			xl.Warnf("Failed to remove custom domain label from pod [%s]: %v", pxy.loginMsg.Hostname, err)
		}
	}
}

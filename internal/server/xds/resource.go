package xdsserver

// resource.go —— xDS 资源构建
//
// 将一条 ProxyRule 构建为 Envoy 的 protobuf 资源

import (
	"fmt"
	"time"

	xdscore "github.com/cncf/xds/go/xds/core/v3"
	matcher "github.com/cncf/xds/go/xds/type/matcher/v3"
	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	udpproxy "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/udp/udp_proxy/v3"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

// buildOneRule 根据协议类型分发
func buildOneRule(rule *ProxyRule, connectTimeout, udpIdleTimeout time.Duration) (*ruleRes, error) {
	switch rule.Protocol {
	case ProtocolUDP:
		return buildUDPRule(rule, connectTimeout, udpIdleTimeout)
	default:
		return buildHTTPRule(rule, connectTimeout)
	}
}

// ─── HTTP ──────────────────────────────────────────────────────────────

func buildHTTPRule(rule *ProxyRule, connectTimeout time.Duration) (*ruleRes, error) {
	clusterName := "cluster_" + rule.Name
	routeName := "route_" + rule.Name

	// EDS
	lbEndpoints := make([]*endpoint.LbEndpoint, 0, len(rule.Backends))
	for _, b := range rule.Backends {
		lbEndpoints = append(lbEndpoints, &endpoint.LbEndpoint{
			HostIdentifier: &endpoint.LbEndpoint_Endpoint{
				Endpoint: &endpoint.Endpoint{
					Address: socketAddr(b.Address, b.Port),
				},
			},
			LoadBalancingWeight: &wrapperspb.UInt32Value{Value: b.Weight},
		})
	}

	ep := &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpoint.LocalityLbEndpoints{{
			LbEndpoints:         lbEndpoints,
			LoadBalancingWeight: &wrapperspb.UInt32Value{Value: 1},
		}},
	}

	// CDS
	cl := &cluster.Cluster{
		Name:                 clusterName,
		ConnectTimeout:       durationpb.New(connectTimeout),
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
		EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
			EdsConfig: adsSource(),
		},
		LbPolicy: parseLBPolicy(rule.LBPolicy),
	}

	// RDS
	rt := &route.RouteConfiguration{
		Name: routeName,
		VirtualHosts: []*route.VirtualHost{{
			Name:    "vh_" + rule.Name,
			Domains: []string{"*"},
			Routes: []*route.Route{{
				Match: &route.RouteMatch{
					PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/"},
				},
				Action: &route.Route_Route{
					Route: &route.RouteAction{
						ClusterSpecifier: &route.RouteAction_Cluster{
							Cluster: clusterName,
						},
					},
				},
			}},
		}},
	}

	// LDS
	routerAny, err := mustAny(&router.Router{})
	if err != nil {
		return nil, fmt.Errorf("序列化 Router 过滤器配置失败: %w", err)
	}
	hcmAny, err := mustAny(&hcm.HttpConnectionManager{
		StatPrefix: "ingress_" + rule.Name,
		CodecType:  hcm.HttpConnectionManager_AUTO,
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{
			Rds: &hcm.Rds{
				ConfigSource:    adsSource(),
				RouteConfigName: routeName,
			},
		},
		HttpFilters: []*hcm.HttpFilter{{
			Name: "envoy.filters.http.router",
			ConfigType: &hcm.HttpFilter_TypedConfig{
				TypedConfig: routerAny,
			},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("序列化 HTTP 连接管理器配置失败: %w", err)
	}

	li := &listener.Listener{
		Name:    "listener_" + rule.Name,
		Address: socketAddr(rule.ListenAddr, rule.ListenPort),
		FilterChains: []*listener.FilterChain{{
			Filters: []*listener.Filter{{
				Name: "envoy.filters.network.http_connection_manager",
				ConfigType: &listener.Filter_TypedConfig{
					TypedConfig: hcmAny,
				},
			}},
		}},
	}

	return &ruleRes{
		owner: rule, endpoint: ep, cluster: cl, route: rt, listener: li,
	}, nil
}

// ─── UDP ───────────────────────────────────────────────────────────────

func buildUDPRule(rule *ProxyRule, connectTimeout, udpIdleTimeout time.Duration) (*ruleRes, error) {
	clusterName := "cluster_" + rule.Name

	lbEndpoints := make([]*endpoint.LbEndpoint, 0, len(rule.Backends))
	for _, b := range rule.Backends {
		lbEndpoints = append(lbEndpoints, &endpoint.LbEndpoint{
			HostIdentifier: &endpoint.LbEndpoint_Endpoint{
				Endpoint: &endpoint.Endpoint{
					Address: socketAddr(b.Address, b.Port),
				},
			},
			LoadBalancingWeight: &wrapperspb.UInt32Value{Value: b.Weight},
		})
	}

	cl := &cluster.Cluster{
		Name:                 clusterName,
		ConnectTimeout:       durationpb.New(connectTimeout),
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_STATIC},
		LbPolicy:             parseLBPolicy(rule.LBPolicy),
		LoadAssignment: &endpoint.ClusterLoadAssignment{
			ClusterName: clusterName,
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				LbEndpoints:         lbEndpoints,
				LoadBalancingWeight: &wrapperspb.UInt32Value{Value: 1},
			}},
		},
	}

	udpRouteAny, err := mustAny(&udpproxy.Route{Cluster: clusterName})
	if err != nil {
		return nil, fmt.Errorf("序列化 UDP 路由配置失败: %w", err)
	}
	udpProxyAny, err := mustAny(&udpproxy.UdpProxyConfig{
		StatPrefix: "ingress_" + rule.Name,
		RouteSpecifier: &udpproxy.UdpProxyConfig_Matcher{
			Matcher: &matcher.Matcher{
				OnNoMatch: &matcher.Matcher_OnMatch{
					OnMatch: &matcher.Matcher_OnMatch_Action{
						Action: &xdscore.TypedExtensionConfig{
							Name:        "route_action",
							TypedConfig: udpRouteAny,
						},
					},
				},
			},
		},
		IdleTimeout: durationpb.New(udpIdleTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("序列化 UDP 代理配置失败: %w", err)
	}

	li := &listener.Listener{
		Name:              "listener_" + rule.Name,
		Address:           udpSocketAddr(rule.ListenAddr, rule.ListenPort),
		UdpListenerConfig: &listener.UdpListenerConfig{},
		ListenerFilters: []*listener.ListenerFilter{{
			Name: "envoy.filters.udp_listener.udp_proxy",
			ConfigType: &listener.ListenerFilter_TypedConfig{
				TypedConfig: udpProxyAny,
			},
		}},
	}

	return &ruleRes{
		owner: rule, endpoint: nil, cluster: cl, route: nil, listener: li,
	}, nil
}

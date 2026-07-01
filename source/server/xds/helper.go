package xdsserver

// helper.go —— 工具函数

import (
	"fmt"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	proto "google.golang.org/protobuf/proto"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

// socketAddr 构造 TCP 协议的 Envoy SocketAddress 资源。
func socketAddr(addr string, port uint32) *core.Address {
	return &core.Address{
		Address: &core.Address_SocketAddress{
			SocketAddress: &core.SocketAddress{
				Address: addr,
				PortSpecifier: &core.SocketAddress_PortValue{
					PortValue: port,
				},
			},
		},
	}
}

// udpSocketAddr 构造 UDP 协议的 Envoy SocketAddress 资源。
func udpSocketAddr(addr string, port uint32) *core.Address {
	return &core.Address{
		Address: &core.Address_SocketAddress{
			SocketAddress: &core.SocketAddress{
				Address: addr,
				PortSpecifier: &core.SocketAddress_PortValue{
					PortValue: port,
				},
				Protocol: core.SocketAddress_UDP,
			},
		},
	}
}

// adsSource 返回 ADS（Aggregated Discovery Service）配置源。
func adsSource() *core.ConfigSource {
	return &core.ConfigSource{
		ResourceApiVersion: resourcev3.DefaultAPIVersion,
		ConfigSourceSpecifier: &core.ConfigSource_Ads{
			Ads: &core.AggregatedConfigSource{},
		},
	}
}

// mustAny 将 protobuf 消息包装为 google.protobuf.Any 类型，失败时返回错误。
func mustAny(msg proto.Message) (*anypb.Any, error) {
	a, err := anypb.New(msg)
	if err != nil {
		return nil, fmt.Errorf("anypb.New: %w", err)
	}
	return a, nil
}

// parseLBPolicy 将负载均衡策略字符串转换为 Envoy Cluster 枚举值。
func parseLBPolicy(s string) cluster.Cluster_LbPolicy {
	switch s {
	case "LEAST_REQUEST":
		return cluster.Cluster_LEAST_REQUEST
	case "RANDOM":
		return cluster.Cluster_RANDOM
	case "RING_HASH":
		return cluster.Cluster_RING_HASH
	default:
		return cluster.Cluster_ROUND_ROBIN
	}
}

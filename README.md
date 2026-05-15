# Envoy xDS 动态代理控制面
https://github.com/meguoe/envoy.git
通过 HTTP API 动态创建/删除代理规则，实时生效，零重启。

## 架构

```
┌─────────────┐         ┌────────────────────┐         ┌──────────────┐
│   Client    │         │    Envoy Proxy     │         │  Upstream    │
│             │◀═══════▶│  :9000, :9001 ...  │◀═══════▶│  :8081 ...   │
└─────────────┘         └────────┬───────────┘         └──────────────┘
                                 │
                                 │ ADS gRPC stream
                                 ▼
                       ┌────────────────────┐
                       │   xDS Server       │
                       │ gRPC :18000 (ADS)  │
                       │ HTTP :18001 (API)  │◀── 管理代理规则
                       └────────────────────┘
```

## 快速开始

```bash
# 启动 xDS 控制面
go mod tidy && go run .

# 启动 Envoy
envoy -c envoy.yaml --log-level info
```

# Envoy 配置

```yaml
# envoy.yaml (minimal bootstrap)
admin:
  address:
    socket_address: { address: 0.0.0.0, port_value: 9901 }

dynamic_resources:
  ads_config:
    api_type: GRPC
    transport_api_version: V3
    grpc_services:
      - envoy_grpc:
          cluster_name: xds-cluster
  lds_config:
    ads: {}
  cds_config:
    ads: {}

static_resources:
  clusters:
    - name: xds-cluster
      type: STATIC
      connect_timeout: 1s
      typed_extension_protocol_options:
        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
          "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
          explicit_http_config:
            http2_protocol_options: {}
      load_assignment:
        cluster_name: xds-cluster
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: 127.0.0.1, port_value: 18000 }

```

## API 接口

### 创建代理规则

```bash
POST /rules
Content-Type: application/json

{
  "name":           "web",                # 规则名称（唯一标识）
  "listen_port":    9981,                 # Envoy 监听端口
  "listen_addr":    "0.0.0.0",          # Envoy 监听地址
  "backends": [
    {
      "address":  "124.236.16.69",      # 上游地址
      "port": 7073,                     # 上游端口
      "weight": 1                       # 加权负载均衡权重，0 等效于 1
    }
  ],
  "lb_policy":      "ROUND_ROBIN"       # 轮询策略
}
```

支持的 `lb_policy`：
| 值 | 说明 |
|---|------|
| `ROUND_ROBIN` | 轮询（默认） |
| `LEAST_REQUEST` | 最少连接 |
| `RANDOM` | 随机 |
| `RING_HASH` | 一致性哈希 |

响应：
```json
{
  "code": 201,
  "success": true,
  "message": "ok",
  "data": {
    "id": "xxxx",
    "name": "web",
    "listen_addr": "0.0.0.0",
    "listen_port": 9981,
    "backends": [
      {"address": "124.236.16.69", "port": 7073, "weight": 1}
    ],
    "lb_policy": "ROUND_ROBIN"
  }
}
```

### 删除规则

```bash
DELETE /rules/{id}
curl -X DELETE http://localhost:18001/rules/xxxx
```

响应：
```json
{
  "code": 200,
  "success": true,
  "message": "ok",
  "data": {
    "id": "xxxx",
  }
}
```

### 更新规则

```bash
PUT /rules/{id}
curl -X PUT http://localhost:18001/rules/a57f656a42a6ebb2
Content-Type: application/json

{
  "listen_port": 9982,
  "listen_addr":    "0.0.0.0",
  "backends": [
    {
      "address":  "124.236.16.69",
      "port": 7073,
      "weight": 1
    }
  ],
  "lb_policy": "ROUND_ROBIN"
}

```
响应：
```json
{
  "code": 200,
  "success": true,
  "message": "ok",
  "data": {
    "id": "xxxx",
    "name": "web",
    "listen_addr": "0.0.0.0",
    "listen_port": 9982,
    "backends": [
      {"address": "124.236.16.69", "port": 7073, "weight": 1}
    ],
    "lb_policy": "ROUND_ROBIN"
  }
}
```

### 列出所有规则

```bash
GET /rules
curl http://localhost:18001/rules
```


响应：
```json
{
  "code": 200,
  "success": true,
  "message": "ok",
  "data": [
    {
      "id": "xxxx",
      "name": "web",
      "listen_addr": "0.0.0.0",
      "listen_port": 9982,
      "backends": [
        {"address": "124.236.16.69", "port": 7073, "weight": 1}
      ],
      "lb_policy": "ROUND_ROBIN"
    }
  ]
}
```


## 完整示例

```bash

# 创建代理规则
curl -X POST http://localhost:18001/rules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "web",
    "listen_port": 9981,
    "listen_addr": "0.0.0.0",
    "backends": [
      {
        "address": "124.236.16.69",
        "port": 7073,
        "weight": 1
      },
      {
        "address": "124.236.16.70",
        "port": 7073,
        "weight": 2
      }
    ],
    "lb_policy": "RING_HASH"
  }'

# 验证规则生效
curl http://localhost:9981/

# 更新现有规则
curl -X PUT http://localhost:18001/rules/682bcd0507c4ec23 \
  -H 'Content-Type: application/json' \
  -d '{
    "listen_port":9982,
    "listen_addr": "0.0.0.0",
    "backends": [
      {
        "address": "124.236.16.69",
        "port": 7073,
        "weight": 1
      }
    ],
    "lb_policy":"ROUND_ROBIN"
  }'

# 验证规则生效
curl http://localhost:9982/

# 查看所有规则
curl http://localhost:18001/rules

# 删除规则
curl -X DELETE http://localhost:18001/rules/682bcd0507c4ec23

# 查看 Envoy 配置
curl -s http://localhost:9901/config_dump | python3 -m json.tool
```

## 验证清单

| # | 验证项 | 方法 | 预期 |
|---|--------|------|------|
| 1 | 创建规则 | `POST /rules` | 201 Created |
| 2 | 代理生效 | `curl localhost:9000` | 返回上游响应 |
| 3 | 多规则隔离 | 创建 2 个规则各监听不同端口 | 各自独立代理 |
| 4 | 删除规则 | `DELETE /rules/web` | Envoy 移除该 listener |
| 5 | 轮询策略 | 指定 `LEAST_REQUEST` | config_dump 确认 |

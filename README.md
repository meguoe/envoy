# Envoy xDS 动态代理控制面

一个小型 Go xDS control plane。通过 HTTP API 写入代理规则，控制面生成 Envoy v3 xDS 资源并通过 Delta ADS 推送给 Envoy。

Envoy API 文档：https://www.envoyproxy.io/docs/envoy/v1.38.3/api-v3/api

## 当前能力

- 支持 HTTP 代理规则：LDS + RDS + CDS + EDS。
- 支持 UDP 代理规则：LDS + STATIC CDS。
- Envoy 到控制面使用 gRPC mTLS。
- HTTP 管理 API 支持 HTTPS、IP 白名单、`X-API-KEY`、速率限制、请求体大小限制。
- 规则持久化到 PostgreSQL 数据库。
- 暴露 `/health`、`/nodes`、`/metrics`。

不支持：域名路由、多路由匹配、TLS 下游终止、Web UI。

## 目录结构

```text
cmd/control-plane/main.go       # 启动、配置加载、服务器生命周期
internal/config/                # config.yaml 加载和校验
internal/store/                 # PostgreSQL 持久化
internal/server/http/           # 管理 API、认证、限流、日志、metrics
internal/server/xds/            # 规则模型、xDS 引擎、资源构建、mTLS
config.yaml                     # 控制面配置
envoy.yaml                      # 本地 Envoy bootstrap
tools/generate-certs.sh         # 本地测试证书生成
```

## 快速开始

```bash
# 1. 生成证书
./tools/generate-certs.sh

# 2. 启动控制面（数据库需预先存在，表会自动创建）
go run ./cmd/control-plane

# 3. 启动 Envoy
envoy -c envoy.yaml --log-level info
```

当前 `config.yaml` 开启了 HTTPS，所以管理 API 默认使用：

```bash
curl -k https://127.0.0.1:18000/health
```

如果把 `https.enabled` 改成 `false`，再使用：

```bash
curl http://127.0.0.1:18000/health
```

## 编译和检查

```bash
go build -o xds-control-plane ./cmd/control-plane
go test ./...
go vet ./...
```

如果本地沙箱限制 Go 默认 cache：

```bash
GOCACHE=/private/tmp/envoy-go-cache go test ./...
GOCACHE=/private/tmp/envoy-go-cache go build -o xds-control-plane ./cmd/control-plane
```

## 启动参数

```bash
go run ./cmd/control-plane -config config.yaml
go run ./cmd/control-plane -json-log
```

- `-config`：指定配置文件，默认 `config.yaml`。
- `-json-log`：HTTP 访问日志输出 JSON。

## 配置

当前配置以 `config.yaml` 为准。README 只说明字段语义，不复制配置内容。

- `api_addr` 是管理 API 地址。
- `grpc_addr` 是 Envoy xDS ADS 地址。
- `database.*` 配置 PostgreSQL 数据库连接。
- `log_level` 控制普通日志级别：`DEBUG`、`INFO`、`WARN`、`ERROR`。
- `max_body_bytes` 限制管理 API 请求体大小。
- `connect_timeout` 用于生成 Envoy cluster 的连接超时。
- `udp_idle_timeout` 用于 UDP proxy idle timeout。
- `rate_limit.rps` 和 `rate_limit.burst` 控制每个客户端 IP 的令牌桶限流。
- `http_timeout.*` 控制管理 API 的 header、读、写、空闲超时。
- `tls.*` 是 gRPC mTLS 配置，控制面验证 Envoy 客户端证书 URI SAN 等于 `client_uri`。
- `https.*` 只保护 HTTP 管理 API，不做客户端证书认证。
- `allowed_ips` 为空时跳过 IP 白名单。
- `api_key` 为空时跳过 API Key 校验；非空时只接受 `X-API-KEY` header。
- `trusted_proxies` 只影响可信代理下的 `X-Forwarded-For` 解析和限流 key。

## Envoy Bootstrap

`envoy.yaml` 使用 Delta gRPC ADS：

- `node.id`: `envoy-local`，必须和 `config.yaml` 的 `node_id` 对齐。
- `xds_cluster`: 指向 `127.0.0.1:18001`。
- Envoy 使用 `certs/mtls/client.crt` 和 `client.key` 作为客户端证书。
- Envoy 校验控制面服务端证书 DNS SAN 为 `xds-server`。
- 控制面校验 Envoy 客户端证书 URI SAN 为 `spiffe://local/envoy/envoy-local`。

如果看到 `CERTIFICATE_VERIFY_FAILED: SAN matcher`，先检查 `envoy.yaml` 的 `match_typed_subject_alt_names` 是否匹配服务端证书 SAN。

## 规则模型

```json
{
  "id": "server-generated",
  "name": "web",
  "protocol": "http",
  "listen_addr": "0.0.0.0",
  "listen_port": 9981,
  "backends": [
    {"address": "127.0.0.1", "port": 8080, "weight": 1}
  ],
  "lb_policy": "ROUND_ROBIN"
}
```

字段规则：

- `id`：服务端生成，创建时客户端传入会被忽略。
- `name`：唯一；只允许字母、数字、`_`、`-`，首尾必须是字母或数字。更新时不允许改名。
- `protocol`：`http` 或 `udp`，默认 `http`。
- `listen_addr`：合法 IP 或 DNS 名称。
- `listen_port`：`10..65535`。
- `backends`：至少 1 个；`address` 为合法 IP 或 DNS；`port` 为 `1..65535`；`weight` 为 0 时归一化成 1。
- `lb_policy`：`ROUND_ROBIN`、`LEAST_REQUEST`、`RANDOM`、`RING_HASH`，默认 `ROUND_ROBIN`。

同一个控制面内，`name` 不能重复，`listen_addr + listen_port + protocol` 不能重复。

## HTTP API

统一响应：

```json
{
  "code": 200,
  "success": true,
  "message": "ok",
  "data": {}
}
```

### 创建规则

```bash
curl -k -X POST https://127.0.0.1:18000/rules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "web",
    "protocol": "http",
    "listen_addr": "0.0.0.0",
    "listen_port": 9981,
    "backends": [
      {"address": "127.0.0.1", "port": 8080, "weight": 1}
    ],
    "lb_policy": "ROUND_ROBIN"
  }'
```

### UDP 规则

```bash
curl -k -X POST https://127.0.0.1:18000/rules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "dns",
    "protocol": "udp",
    "listen_addr": "0.0.0.0",
    "listen_port": 1053,
    "backends": [
      {"address": "127.0.0.1", "port": 53}
    ]
  }'
```

### 更新规则

```bash
curl -k -X PUT https://127.0.0.1:18000/rules/{id} \
  -H 'Content-Type: application/json' \
  -d '{
    "listen_port": 9982,
    "backends": [
      {"address": "127.0.0.1", "port": 8081}
    ]
  }'
```

更新语义：

- URL 里的 `{id}` 为准，body 里的 `id` 被忽略。
- `name` 继承旧值，不允许通过更新修改。
- 未传的 `protocol`、`listen_addr`、`listen_port`、`lb_policy`、`backends` 会继承旧值。
- body 后有多余 JSON 内容会返回 400。

### 查询和删除

```bash
curl -k https://127.0.0.1:18000/rules
curl -k https://127.0.0.1:18000/rules/{id}
curl -k -X DELETE https://127.0.0.1:18000/rules/{id}
```

### 健康和节点

```bash
curl -k https://127.0.0.1:18000/health
curl -k https://127.0.0.1:18000/nodes
curl -k https://127.0.0.1:18000/metrics
```

`/health` 返回规则数、Envoy 连接状态、连续持久化失败次数、运行时间、HTTP 请求和错误计数。

`/nodes` 只返回当前持有活跃 xDS watch 的 Envoy 节点。

`/metrics` 输出 JSON，包含 HTTP 请求计数、限流计数和 gRPC 请求计数。

gRPC 指标里，`connections_total` 是累计建立的 stream 数，`active_connections` 是当前活跃 stream 数。

## 认证和限流

API Key 示例：

```bash
curl -k -H 'X-API-KEY: your-secret' https://127.0.0.1:18000/rules
```

不会接受 `?api_key=`，避免密钥进入 URL 日志。

限流默认每个客户端 IP 每秒 20 次，突发 40 次。只有请求来源命中 `trusted_proxies` 时，限流和 IP 白名单才会使用 `X-Forwarded-For` 解析真实客户端 IP；否则只使用 `RemoteAddr`。

## 持久化

规则持久化到 PostgreSQL 数据库。

在 `config.yaml` 中配置 `database.*`。数据库需要预先存在，首次启动会自动创建表。

### 存储细节

- 数据库是唯一的规则来源。HTTP API 和人工直接修改 DB 都会生效，控制面每 5 秒轮询检测变化。
- HTTP API 事务写入规则并递增 revision，推送由后台轮询器异步完成。
- 使用事务确保原子性：先删除再批量插入。
- 保存前按 `id` 排序。
- HTTP 返回成功只代表规则已进数据库，不代表 Envoy 已生效。Envoy 实际生效需要等待轮询器推送 + ACK。
- 控制面每 5 秒检查一次数据库规则 revision；发现变化后重新加载规则、生成快照并推送给 Envoy。

## 日志和排障

普通日志级别由 `log_level` 控制：`DEBUG`、`INFO`、`WARN`、`ERROR`。

启动时应看到：

```text
xDS 控制面就绪  gRPC=127.0.0.1:18001  HTTP=127.0.0.1:18000
```

常见问题：

- `CERTIFICATE_VERIFY_FAILED: SAN matcher`：Envoy 校验的服务端证书 SAN 不匹配，检查 `envoy.yaml` 的 `match_typed_subject_alt_names`。
- `加载 TLS 凭证失败`：先运行 `./tools/generate-certs.sh`，并检查 `config.yaml` 的证书路径。
- `来源 IP 不允许`：当前默认只允许 `127.0.0.1` 访问管理 API。
- API 返回 429：触发 `rate_limit`。
- Envoy 没有代理端口：先看 `/nodes` 是否有 watch，再看规则是否创建成功。

## 生产注意

- 生产不要把管理 API 裸露到公网。
- `api_addr` 绑定非 loopback 时，应设置强 `api_key`，并配置网络层访问控制。
- 只在可信反向代理地址里配置 `trusted_proxies`。
- 使用正式 CA 或 SPIFFE/SPIRE 签发 mTLS 证书。
- `certs/` 默认不应提交。

# Envoy xDS Control Plane

轻量级 Envoy xDS 控制面，基于 Go + PostgreSQL，支持 HTTP API 管理代理规则，通过 gRPC xDS 动态下发配置到 Envoy。

## 特性

- **xDS v3 Delta gRPC**：ADS 聚合发现服务，支持 CDS/LDS/RDS/EDS 全量推送
- **事件驱动推送**：规则变更即时推送，30s 兜底 ticker 保证最终一致
- **PostgreSQL 持久化**：数据库为唯一规则源，支持事务安全的原子修改
- **ACK/NACK 追踪**：完整追踪 Envoy 对每个 revision 的响应状态
- **mTLS 双向认证**：xDS gRPC 和 HTTP API 均支持 TLS
- **HTTP + UDP 协议**：支持 HTTP 代理和 UDP 代理
- **CLI 命令系统**：setup、initdb、config、cert 等便捷命令
- **结构化日志**：支持 JSON 格式输出，便于 ELK/Loki 聚合
- **请求限流**：基于 IP 的令牌桶算法，可配置 RPS 和突发容量

## 目录结构

```
envoy-control-plane/
├── source/
│   ├── cmd/main.go                 # 入口、CLI 命令、服务启动
│   ├── config/
│   │   ├── config.go               # 配置加载、校验、默认值
│   │   ├── setup.go                # 交互式配置向导
│   │   └── cert.go                 # TLS 证书生成（mTLS + HTTPS）
│   ├── server/
│   │   ├── http/                   # HTTP API 层
│   │   │   ├── http.go             # 路由、CRUD 处理、响应格式
│   │   │   ├── auth.go             # API_KEY 认证（支持热更新）
│   │   │   ├── ratelimit.go        # 令牌桶限流（基于 IP）
│   │   │   ├── logger.go           # 标准库 slog 日志配置
│   │   │   ├── metrics.go          # 运行指标计数器
│   │   └── xds/                    # xDS 引擎层
│   │       ├── engine.go           # 核心引擎：规则管理、快照推送、gRPC 服务
│   │       ├── model.go            # 数据模型：ProxyRule、BackendNode、校验
│   │       ├── snapshot.go         # Envoy 资源构建：LDS/RDS/CDS/EDS
│   │       ├── resource.go         # 单条规则的资源构建（HTTP + UDP）
│   │       ├── callbacks.go        # ACK/NACK 追踪
│   │       ├── worker.go           # 事件驱动推送 worker
│   │       ├── cache.go            # 增量快照缓存同步
│   │       ├── tls.go              # gRPC mTLS 配置
│   │       ├── helper.go           # 辅助函数（protobuf 工具）
│   │       └── grpc_interceptor.go # gRPC 拦截器（日志 + 指标）
│   └── store/
│       └── pgstore.go              # PostgreSQL 存储：规则 CRUD + revision 管理
├── config.yaml                     # 主配置文件
├── .env.example                    # 环境变量模板
├── envoy.yaml                      # Envoy bootstrap 配置
├── go.mod                          # Go 模块定义
└── certs/                          # 证书目录（gitignore）
```

## 快速开始

### 前置条件

- Go 1.25+（`go.mod` 当前要求；已用 Go 1.26.3 验证）
- PostgreSQL 14+

### 1. 配置环境

```bash
# 交互式配置向导（数据库、API 密钥）
xds-control-plane setup

# 或手动复制并编辑
cp .env.example .env
```

### 2. 初始化数据库

```bash
xds-control-plane initdb
```

### 3. 生成证书

```bash
# 生成 mTLS（xDS）+ HTTPS（API）证书
xds-control-plane cert

# 仅生成 mTLS 证书
xds-control-plane cert --mtls

# 仅生成 HTTPS 证书
xds-control-plane cert --https
```

### 4. 启动服务

```bash
xds-control-plane
```

生产环境用 systemd、Docker、supervisor 或 Kubernetes 管理进程。

### 5. 启动 Envoy

```bash
envoy -c envoy.yaml --log-level info
```

## CLI 命令

| 命令 | 说明 |
|------|------|
| `setup` | 交互式配置向导 |
| `initdb` | 初始化数据库表结构 |
| `initdb --force` | 强制重建数据库（需确认） |
| `config --init` | 生成默认配置文件和证书 |
| `config --validate` | 校验配置文件 |
| `cert` | 生成所有证书（mTLS + HTTPS） |
| `cert --mtls` | 仅生成 mTLS 证书 |
| `cert --https` | 仅生成 HTTPS 证书 |

## 配置

### config.yaml

```yaml
server:
    node_id: envoy-local        # Envoy 节点 ID
    api_addr: 127.0.0.1:18000   # HTTP API 地址
    grpc_addr: 127.0.0.1:18001  # xDS gRPC 地址
    log_level: INFO             # 日志级别: DEBUG, INFO, WARN, ERROR

xds:
    connect_timeout: 1s         # Envoy 连接超时
    udp_idle_timeout: 60s       # UDP 空闲超时
    tls:
        enabled: true
        ca_cert: "certs/mtls/ca.crt"
        server_key: "certs/mtls/server.key"
        server_cert: "certs/mtls/server.crt"
        client_uri: "spiffe://local/envoy/envoy-local"

api:
    max_body_bytes: 5242880     # 请求体上限 (5MB)
    auth:
        enabled: true           # 启用 API_KEY 认证
    tls:
        enabled: true
        key_file: "certs/https/server.key"
        cert_file: "certs/https/server.crt"
    rate_limit:
        rps: 20                 # 每秒请求数
        burst: 40               # 突发容量
    timeout:
        read_timeout: 10s
        write_timeout: 10s
        idle_timeout: 60s
        read_header_timeout: 5s
```

### .env（敏感配置）

```bash
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=envoy_cp
DB_SSLMODE=disable   # 本地可用 disable；生产跨主机建议 require 或 verify-full
API_KEY=your_api_key
```

## HTTP API

所有 API 请求需要 `X-API-KEY` 头认证（当 `api.auth.enabled=true`）。

### 健康检查

```bash
curl http://127.0.0.1:18000/health
```

### 规则管理

```bash
# 创建规则
curl -X POST http://127.0.0.1:18000/rules \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: your_key" \
  -d '{
    "name": "web-service",
    "protocol": "http",
    "listen_addr": "0.0.0.0",
    "listen_port": 8080,
    "backends": [
      {"address": "10.0.0.1", "port": 80, "weight": 1},
      {"address": "10.0.0.2", "port": 80, "weight": 1}
    ],
    "lb_policy": "ROUND_ROBIN"
  }'

# 列出所有规则
curl http://127.0.0.1:18000/rules \
  -H "X-API-KEY: your_key"

# 获取单条规则
curl http://127.0.0.1:18000/rules/{id} \
  -H "X-API-KEY: your_key"

# 更新规则
curl -X PUT http://127.0.0.1:18000/rules/{id} \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: your_key" \
  -d '{
    "backends": [
      {"address": "10.0.0.3", "port": 80, "weight": 2}
    ]
  }'

# 删除规则
curl -X DELETE http://127.0.0.1:18000/rules/{id} \
  -H "X-API-KEY: your_key"
```

### 其他端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 + 运行状态 |
| `/metrics` | GET | HTTP + gRPC 指标 |
| `/nodes` | GET | 已连接的 Envoy 节点信息 |

## 架构

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  HTTP API   │────▶│  PostgreSQL  │◀────│  CLI Setup  │
│  :18000     │     │  规则存储     │     │  initdb     │
└──────┬──────┘     └──────────────┘     └─────────────┘
       │
       ▼
┌──────────────┐     ┌──────────────┐     ┌─────────────┐
│  xDS Engine  │────▶│  gRPC Server │────▶│   Envoy     │
│  规则→快照    │     │  :18001      │     │  mTLS 客户端 │
└──────┬──────┘     └──────────────┘     └─────────────┘
       │
       ▼
┌──────────────┐
│ Push Worker  │
│ 事件+兜底     │
└──────────────┘
```

核心数据流：HTTP API → PostgreSQL (事务) → Worker 通知 → xDS Engine → gRPC → Envoy

## 开发

```bash
# 编译
go build -o xds-control-plane ./source/cmd

# 运行测试
go test ./...

# 运行指定包测试
go test -v ./source/server/xds/...
go test -run TestValidateRule ./source/server/xds/...

# 格式化代码
gofmt -w .

# 前台运行（调试）
go run ./source/cmd

# 以 JSON 日志运行
go run ./source/cmd -json-log
```

## 数据模型

### ProxyRule

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 规则名，仅允许字母、数字、下划线、短横线、点号，且首尾必须为字母或数字 |
| `protocol` | string | 否 | 协议类型：`http`（默认）、`udp` |
| `listen_addr` | string | 是 | 监听地址，IP 或 DNS 名称 |
| `listen_port` | uint32 | 是 | 监听端口，范围 10-65535 |
| `backends` | array | 是 | 后端节点列表，至少一个 |
| `lb_policy` | string | 否 | 负载均衡策略：`ROUND_ROBIN`（默认）、`LEAST_REQUEST`、`RANDOM`、`RING_HASH` |

### BackendNode

| 字段 | 类型 | 说明 |
|------|------|------|
| `address` | string | 后端地址，IP 或 DNS 名称 |
| `port` | uint32 | 后端端口 |
| `weight` | uint32 | 权重，默认 1 |

## Envoy Bootstrap

`envoy.yaml` 配置 Envoy 使用 Delta gRPC ADS 连接控制面：

- Node ID: `envoy-local`
- xDS 集群: `127.0.0.1:18001` (mTLS)
- 重试策略: 1s-5s 指数退避
- 断路器: 最大 4 连接 / 64 请求 / 32 待处理 / 8 重试

## License

MIT

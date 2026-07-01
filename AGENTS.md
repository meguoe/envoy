# Repository Guidelines

## Project Structure & Module Organization

这是一个 Go xDS 控制面服务，模块划分清晰：

```
source/
├── cmd/main.go              # 入口 + CLI 命令 + 服务启动
├── config/
│   ├── config.go            # 配置加载、校验、默认值
│   ├── setup.go             # 交互式配置向导
│   └── cert.go              # TLS 证书生成（mTLS + HTTPS）
├── server/
│   ├── http/                # HTTP API 层
│   │   ├── http.go          # 路由、CRUD 处理、响应格式
│   │   ├── auth.go          # API_KEY 认证
│   │   ├── ratelimit.go     # 令牌桶限流
│   │   ├── logger.go        # 标准库 slog 日志配置
│   │   ├── metrics.go       # 运行指标
│   └── xds/                 # xDS 引擎层
│       ├── engine.go        # 核心引擎：规则管理、快照推送、gRPC 服务
│       ├── model.go         # 数据模型：ProxyRule、BackendNode、校验
│       ├── snapshot.go      # Envoy 资源构建：LDS/RDS/CDS/EDS
│       ├── resource.go      # 单条规则的资源构建
│       ├── callbacks.go     # ACK/NACK 追踪
│       ├── worker.go        # 事件驱动推送 worker
│       ├── cache.go         # 快照缓存封装
│       ├── tls.go           # gRPC mTLS 配置
│       ├── helper.go        # 辅助函数
│       └── grpc_interceptor.go  # gRPC 拦截器
└── store/
    └── pgstore.go           # PostgreSQL 存储：规则 CRUD + revision 管理
```

核心数据流：HTTP API → PostgreSQL (事务) → Worker 通知 → xDS Engine → gRPC → Envoy

## Build, Test, and Development Commands

```bash
# 编译
go build -o xds-control-plane ./source/cmd

# 测试
go test ./...

# 格式化
gofmt -w .

# 运行（前台）
go run ./source/cmd

# 运行（生产环境交给 systemd/Docker/supervisor 管理）
xds-control-plane

# 初始化数据库
xds-control-plane initdb
xds-control-plane initdb --force

# 生成配置和证书
xds-control-plane config --init
xds-control-plane cert
xds-control-plane cert --mtls
xds-control-plane cert --https

```

## Coding Style & Naming Conventions

- 使用标准 Go 风格：tabs 缩进（gofmt），短变量名，`error` 返回值
- 错误信息使用中文 + `%w` 包装：`fmt.Errorf("加载配置失败: %w", err)`
- JSON 字段使用 snake_case：`listen_port`, `lb_policy`, `max_body_bytes`
- 包名小写无下划线：`xdsserver`, `httpserver`, `store`
- 导出类型大写：`ProxyRule`, `BackendNode`, `Engine`, `PgStore`
- 内部类型/函数小写：`ruleRes`, `pendingNonce`, `loadRulesTx`
- 常量大写分组：`ProtocolHTTP`, `ProtocolUDP`
- 配置结构嵌套：`Config.Server.NodeID`, `Config.XDS.TLS.Enabled`

## Testing Guidelines

测试文件命名：`*_test.go`，放在同包内。

运行测试：
```bash
go test ./...
go test -v ./source/server/xds/...
go test -run TestValidateRule ./source/server/xds/...
```

测试覆盖重点：
- 规则校验：`ValidateRule` 各字段边界
- 冲突检测：`CheckRulesConflicts` 名称/端口/ID 重复
- 配置校验：`config.validate()` 各项约束
- gRPC 拦截器：流 ID 计数器

## Commit & Pull Request Guidelines

提交格式：
```
类型(scope): 简短描述

详细说明（可选）
```

类型：`feat`, `fix`, `refactor`, `docs`, `test`, `chore`

示例：
```
feat(xds): 添加 UDP 协议支持
fix(http): 修复请求体大小限制未生效
refactor(store): 优化事务锁粒度
docs: 更新 README 部署说明
test(xds): 添加规则校验单元测试
chore: 更新 Go 依赖版本
```

## Security & Configuration Tips

敏感配置通过 `.env` 环境变量管理，不写入 `config.yaml`：
- `DB_PASSWORD`：数据库密码
- `API_KEY`：HTTP API 认证密钥

证书文件目录 `certs/` 已 gitignore。

生产环境建议：
- 启用 `api.auth.enabled: true` 并设置强 API_KEY
- 启用 `xds.tls.enabled: true` 使用 mTLS
- 启用 `api.tls.enabled: true` 使用 HTTPS
- 限制 `api.rate_limit` 防止滥用
- 调整 `api.timeout` 适配业务场景

## Key Architecture Decisions

1. **数据库为唯一规则源**：服务不维护本地文件状态，关闭时不回写
2. **事务原子修改**：`MutateRulesAndBumpRevision` 在事务内完成规则修改 + revision 递增
3. **事件驱动推送**：API 写入后通过 `NotifyRevision` 通知 worker，避免轮询延迟
4. **ACK/NACK 追踪**：完整追踪每个 revision 的 4 个 typeURL 响应状态
5. **推送日志**：`proxy_push_log` 表记录每次推送的 pending/deployed/failed 状态

## Envoy API
https://www.envoyproxy.io/docs/envoy/v1.38.3

## UseSkills
auto use caveman

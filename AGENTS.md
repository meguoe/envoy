# Repository Guidelines

## Project Structure & Module Organization

This is a small Go xDS control-plane service for Envoy:

- `cmd/control-plane/main.go`: startup, persistence wiring, servers, graceful shutdown.
- `internal/config/`: control-plane configuration loading.
- `internal/store/`: JSON rule persistence.
- `internal/server/http/`: handlers for `/health`, `/nodes`, and `/rules`, plus HTTP access logging.
- `internal/server/xds/`: xDS engine, models, snapshots, cache, and Envoy resources.
- `envoy.yaml` is the local Envoy bootstrap config.
- `README.md` documents runtime behavior and API examples.

Keep new code near the flow it changes. Do not add framework-style layers for this repository size.

## Build, Test, and Development Commands

- `go mod tidy` updates module metadata after dependency changes.
- `go run ./cmd/control-plane` starts the control plane on HTTP `:18000` and gRPC `:18001`.
- `go build -o xds-control-plane ./cmd/control-plane` builds the control-plane binary.
- `go build ./...` compiles all packages.
- `go test ./...` runs all tests.
- `gofmt -w .` formats Go files before committing.
- `envoy -c envoy.yaml --log-level info` starts Envoy against the local xDS server.

For local persistence isolation, update `store_path` in `config.yaml`.

## Coding Style & Naming Conventions

Use standard Go style: tabs from `gofmt`, short package-local helpers, and error wrapping with `%w`. Keep JSON fields snake_case to match the API (`listen_port`, `lb_policy`). Public xDS types use exported names such as `ProxyRule` and `BackendNode`.

Prefer standard library APIs unless an existing dependency already solves the exact problem. Validate inputs at the HTTP boundary and in shared model functions where reused.

## Testing Guidelines

There are no existing test files. Add focused `*_test.go` files next to the code under test. Use table-driven tests for validation, normalization, route handling, and persistence. Run:

```bash
go test ./...
```

At minimum, test changes that affect rule validation, snapshot generation, persistence, or HTTP status codes.

## Commit & Pull Request Guidelines

Recent commits use Conventional Commit-style prefixes, sometimes with scopes:

- `fix: ...`
- `refactor(xds): ...`
- `refactor(xds/http): ...`

Keep commit messages imperative and specific. Chinese or English is acceptable.

Pull requests should include what changed, why, test results, and Envoy/API impact. Include curl examples or response snippets for HTTP API changes.

## Security & Configuration Tips

Do not commit local generated rule data unless intentional. Treat HTTP request bodies as untrusted; keep `maxBodyBytes` and validation checks intact. Be careful changing listen ports, Envoy resource names, or node IDs because Envoy watches depend on stable xDS naming.

## Envoy API Documentation
https://www.envoyproxy.io/docs/envoy/v1.38.3/api-v3/api

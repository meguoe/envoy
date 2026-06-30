#!/bin/bash
# generate-certs.sh — 生成 mTLS / HTTPS 测试证书（仅用于本地开发）
#
# 使用方法：
#   ./generate-certs.sh          # 同时生成 mTLS 和 HTTPS 证书
#   ./generate-certs.sh mtls     # 仅生成 mTLS 证书
#   ./generate-certs.sh https    # 仅生成 HTTPS 证书
#
# 生成文件（mTLS）：
#   certs/mtls/ca.crt          - CA 证书
#   certs/mtls/server.crt      - 服务器证书（Envoy xDS）
#   certs/mtls/server.key      - 服务器私钥
#   certs/mtls/client.crt      - 客户端证书（Envoy）
#   certs/mtls/client.key      - 客户端私钥（Envoy）
#
# 生成文件（HTTPS）：
#   certs/https/ca.crt         - CA 证书
#   certs/https/server.crt     - 服务器证书（HTTP API）
#   certs/https/server.key     - 服务器私钥

set -e

DAYS=3650
BASE_DIR="./certs"

usage() {
  echo "Usage: $0 [mtls|https|all]"
  echo ""
  echo "  mtls  - 仅生成 mTLS 证书（Envoy xDS 双向认证）"
  echo "  https - 仅生成 HTTPS 证书（HTTP API 服务端认证）"
  echo "  all   - 同时生成两种证书（默认）"
  exit 1
}

generate_ca() {
  local dir="$1" label="$2"
  echo "  [$label] 生成 CA 证书..."
  openssl genrsa -out "$dir/ca.key" 2048 2>/dev/null
  openssl req -new -x509 -days "$DAYS" -key "$dir/ca.key" \
    -out "$dir/ca.crt" \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Envoy Control Plane/CN=$label CA" \
    2>/dev/null
}

generate_server_cert() {
  local dir="$1" cn="$2" san_file="$3"
  openssl genrsa -out "$dir/server.key" 2048 2>/dev/null
  openssl req -new -key "$dir/server.key" \
    -out "$dir/server.csr" \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Envoy Control Plane/CN=$cn" \
    2>/dev/null
  openssl x509 -req -in "$dir/server.csr" \
    -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -out "$dir/server.crt" -days "$DAYS" \
    -extfile "$san_file" \
    2>/dev/null
}

cleanup() {
  rm -f "$1"/*.csr "$1"/*.ext "$1"/*.srl "$1/ca.key"
}

generate_mtls() {
  local dir="$BASE_DIR/mtls"
  echo "=== 生成 mTLS 证书 ==="
  mkdir -p "$dir"

  generate_ca "$dir" "mTLS"

  # 服务器证书 — 用于 xDS gRPC
  echo "  [mTLS] 生成服务器证书..."
  cat > "$dir/server.ext" << 'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1 = xds-server
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF
  generate_server_cert "$dir" "xds-server" "$dir/server.ext"

  # 客户端证书 — 用于 Envoy
  echo "  [mTLS] 生成客户端证书..."
  openssl genrsa -out "$dir/client.key" 2048 2>/dev/null
  openssl req -new -key "$dir/client.key" \
    -out "$dir/client.csr" \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Envoy/CN=envoy-local" \
    2>/dev/null
  cat > "$dir/client.ext" << 'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth
subjectAltName=@alt_names

[alt_names]
URI.1 = spiffe://local/envoy/envoy-local
EOF
  openssl x509 -req -in "$dir/client.csr" \
    -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -out "$dir/client.crt" -days "$DAYS" \
    -extfile "$dir/client.ext" \
    2>/dev/null

  cleanup "$dir"
  echo "  [mTLS] 完成"
  echo ""
}

generate_https() {
  local dir="$BASE_DIR/https"
  echo "=== 生成 HTTPS 证书 ==="
  mkdir -p "$dir"

  generate_ca "$dir" "HTTPS"

  echo "  [HTTPS] 生成服务器证书..."
  cat > "$dir/server.ext" << 'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = http-server
IP.1 = 127.0.0.1
EOF
  generate_server_cert "$dir" "http-server" "$dir/server.ext"

  cleanup "$dir"
  echo "  [HTTPS] 完成"
  echo ""
}

print_summary() {
  local mode="$1"
  echo "=== 证书生成完成 ==="
  echo ""

  if [[ "$mode" == "all" || "$mode" == "mtls" ]]; then
    echo "[mTLS] 文件位置："
    echo "  CA 证书:     $BASE_DIR/mtls/ca.crt"
    echo "  服务器证书:  $BASE_DIR/mtls/server.crt"
    echo "  服务器私钥:  $BASE_DIR/mtls/server.key"
    echo "  客户端证书:  $BASE_DIR/mtls/client.crt"
    echo "  客户端私钥:  $BASE_DIR/mtls/client.key"
    echo ""
  fi

  if [[ "$mode" == "all" || "$mode" == "https" ]]; then
    echo "[HTTPS] 文件位置："
    echo "  CA 证书:     $BASE_DIR/https/ca.crt"
    echo "  服务器证书:  $BASE_DIR/https/server.crt"
    echo "  服务器私钥:  $BASE_DIR/https/server.key"
    echo ""
  fi

  echo "使用方法："
  echo "  # 1. 在 config.yaml 中配置 tls 路径"
  echo "  #    mtls_certs_path:  ./certs/mtls"
  echo "  #    https_certs_path: ./certs/https"
  echo ""
  echo "  # 2. 启动控制平面"
  echo "  go run ./cmd/control-plane"
  echo ""
  echo "  # 3. Envoy 使用 mTLS 客户端证书连接"
  echo "  envoy -c envoy.yaml"
}

# --- main ---
MODE="${1:-all}"

case "$MODE" in
  mtls)  generate_mtls ;;
  https) generate_https ;;
  all)   generate_mtls; generate_https ;;
  *)     usage ;;
esac

print_summary "$MODE"

#!/usr/bin/env bash

# Script for testing Unix domain sockets support. Requires `jq` and `grpcurl`
# installed locally.

set -e

CLI_DIR=$(dirname "$0")
CLI_DIR=$(realpath -L "$CLI_DIR/..")
CLI_TARGET_DIR="$CLI_DIR/target/debug"

function build_grpc_server {
  echo "Building gRPC server CLI app..."
  cargo build --manifest-path="$CLI_DIR/Cargo.toml" --all-features

  if [[ ! -x "$CLI_TARGET_DIR/tardigrade-grpc" ]]; then
    echo "tardigrade-grpc binary not found in expected location $CLI_TARGET_DIR"
    exit 1
  fi
}

case "$1" in
  '')
    ADDRESS=127.0.0.1:9000
    BIND_ADDRESS="$ADDRESS"
    GRPCURL_OPTS='';;
  'uds')
    ADDRESS=/tmp/tardigrade-grpc.sock
    BIND_ADDRESS="sock://$ADDRESS"
    # We need to override `:authority` since the default value is rejected
    # by the server.
    GRPCURL_OPTS='--authority=localhost --unix';;
  *)
    echo "Invalid arg '$1': expected nothing or 'uds'"; exit 1;;
esac

# Preliminary checks for third-party executables
echo "Checking jq..."
jq --version
echo "Checking grpcurl..."
grpcurl --version

build_grpc_server
export PATH=$PATH:$CLI_TARGET_DIR

echo "Killing gRPC server, if any..."
killall -q tardigrade-grpc || echo "gRPC server was not running"
if [[ "$1" == "uds" ]]; then
  rm -f "$ADDRESS"
fi

echo "Starting gRPC server on $ADDRESS..."
RUST_LOG=info,tardigrade_grpc=debug,tardigrade_rt=debug,tardigrade=debug \
  tardigrade-grpc --mock "$BIND_ADDRESS" &
sleep 3 # wait for the server to start

echo "Checking gRPC services..."

grpcurl $GRPCURL_OPTS -plaintext "$ADDRESS" list | grep 'tardigrade.v0.RuntimeService'

echo "Getting runtime info..."
INFO=$(grpcurl $GRPCURL_OPTS -plaintext "$ADDRESS" tardigrade.v0.RuntimeService/GetInfo)
echo "$INFO"
HAS_DRIVER=$(echo "$INFO" | jq '.hasDriver')
if [[ ! "$HAS_DRIVER" == 'true' ]]; then
  echo "Unexpected server response: $INFO"
  exit 1
fi
CLOCK_TYPE=$(echo "$INFO" | jq -r '.clockType')
if [[ ! "$CLOCK_TYPE" == 'CLOCK_TYPE_MOCK' ]]; then
  echo "Unexpected server response: $INFO"
  exit 1
fi

echo "Shutting down server..."
killall -q tardigrade-grpc

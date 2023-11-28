#!/bin/bash

echo "Node id is ${WASM_SERVERLESS_NODEID}"

/usr/local/bin/wasm_serverless $WASM_SERVERLESS_NODEID /etc/wasm_serverless/config/node_config.yaml
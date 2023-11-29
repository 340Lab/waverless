#!/bin/bash

echo "Node id: ${WASM_SERVERLESS_NODEID}"
echo "Who am i: $(whoami)"

cd /usr/local/bin/
ls /etc/wasm_serverless/

wasm_serverless $WASM_SERVERLESS_NODEID /etc/wasm_serverless/
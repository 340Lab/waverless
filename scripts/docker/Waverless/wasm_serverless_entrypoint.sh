#!/bin/bash

echo "Node id: ${WASM_SERVERLESS_NODEID}"
echo "Who am i: $(whoami)"

# tc qdisc add dev eth0 root netem delay 100ms

# tc qdisc add dev eth0 root tbf rate 1mbit burst 10kb latency 70ms

timeout 10 ping baidu.com



/etc/wasm_serverless/wasm_serverless $WASM_SERVERLESS_NODEID /etc/wasm_serverless/test_dir
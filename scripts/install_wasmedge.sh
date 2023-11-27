#!/bin/bash

# curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s -- -v 0.13.3

mkdir /tmp/install
cp docker/WasmEdge/env_prepare/inner/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz /tmp/install/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz

python3 docker/WasmEdge/env_prepare/inner/wasm_edge.py -v 0.13.3
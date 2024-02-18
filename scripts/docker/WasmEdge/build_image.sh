#!/bin/bash

cp scripts/install/inner/wasm_edge.py scripts/docker/WasmEdge
cp scripts/install/inner/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz scripts/docker/WasmEdge

ls scripts/docker/WasmEdge

docker build -t wasm_serverless_env:v1 scripts/docker/WasmEdge --no-cache

rm -f scripts/docker/WasmEdge/wasm_edge.py
rm -f scripts/docker/WasmEdge/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz
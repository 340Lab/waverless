#!/bin/bash

cp scripts/install/inner/wasm_edge.py docker/WasmEdge
cp scripts/install/inner/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz docker/WasmEdge

ls docker/WasmEdge

docker build -t wasm_serverless_env:v1 docker/WasmEdge --no-cache

rm -f docker/WasmEdge/wasm_edge.py
rm -f docker/WasmEdge/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz
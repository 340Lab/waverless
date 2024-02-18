
# FROM busybox:1.36 as builder
# WORKDIR /tmp/build
# COPY . .
# RUN apt-get update && apt-get install -y cmake g++ libprotobuf-dev protobuf-compiler
# RUN cargo build --release

# FROM python as builder

# WORKDIR /tmp/build

FROM ubuntu:18.04 as wasm_serverless
# FROM wasmedge/slim:0.13.3 as wasm_serverless
LABEL maintainers="ActivePeter"
LABEL description="Wasm serverless platform"

RUN apt-get update && apt-get install -y python3 python3-pip git iproute2 iputils-ping && mkdir -p /tmp/install
# COPY --from=builder /root/.wasmedge /root/.wasmedge
COPY wasm_edge.py /tmp/install/wasm_edge.py
COPY WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz /tmp/install/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz

RUN ls /tmp/install
RUN python3 /tmp/install/wasm_edge.py -v 0.13.3

# COPY target/release/wasm_serverless /usr/local/bin/wasm_serverless
# COPY node_config.yaml /etc/wasm_serverless/node_config.yaml
# COPY scripts/wasm_serverless_entrypoint.sh /etc/wasm_serverless/
# RUN chmod +x /etc/wasm_serverless/wasm_serverless_entrypoint.sh

ENTRYPOINT ["echo","helloworld"]
# ENTRYPOINT ["bash","/etc/wasm_serverless/wasm_serverless_entrypoint.sh"]

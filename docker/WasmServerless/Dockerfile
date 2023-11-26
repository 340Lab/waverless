
# FROM busybox:1.36 as builder
# WORKDIR /tmp/build
# COPY . .
# RUN apt-get update && apt-get install -y cmake g++ libprotobuf-dev protobuf-compiler
# RUN cargo build --release

# FROM python as builder

# WORKDIR /tmp/build

FROM wasm_serverless_env:v1 as wasm_serverless
# FROM wasmedge/slim:0.13.3 as wasm_serverless
LABEL maintainers="ActivePeter"
LABEL description="Wasm serverless platform"

# RUN apt-get update && apt-get install -y python3 python3-pip git
# # COPY --from=builder /root/.wasmedge /root/.wasmedge
# COPY scripts/env_prepare/inner/wasm_edge.py /tmp/install/
# RUN python3 /tmp/install/wasm_edge.py -v 0.13.3

COPY target/release/wasm_serverless /usr/local/bin/wasm_serverless
COPY node_config.yaml /etc/wasm_serverless/node_config.yaml
COPY wasm_serverless_entrypoint.sh /etc/wasm_serverless/
RUN chmod +x /etc/wasm_serverless/wasm_serverless_entrypoint.sh
# ENV WASM_SERVERLESS_NODEID=-1

# ENTRYPOINT ["echo","helloworld"]
ENTRYPOINT ["bash","/etc/wasm_serverless/wasm_serverless_entrypoint.sh"]

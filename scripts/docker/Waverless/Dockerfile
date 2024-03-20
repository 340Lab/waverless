
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

COPY wasm_serverless_entrypoint.sh /tmp
RUN chmod +x /tmp/wasm_serverless_entrypoint.sh

ENTRYPOINT ["bash","/tmp/wasm_serverless_entrypoint.sh"]

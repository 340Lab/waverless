#!/bin/bash

CURRENT_DIR=`pwd`
TARGET_DIR="$CURRENT_DIR/target"
DOCKER_DIR="$CURRENT_DIR/docker/WasmServerless"
IMAGE_VERSION="v1"
IMAGE_NAME="wasm_serverless:$IMAGE_VERSION"

if [ ! -d $TARGET_DIR ]
then
    cargo build --release
    if test $? -ne 0
    then
        exit 1
    fi
fi


mkdir -p $DOCKER_DIR/target/release
cp $TARGET_DIR/release/wasm_serverless $DOCKER_DIR/target/release/wasm_serverless

# cp "$CURRENT_DIR/node_config.yaml" $DOCKER_DIR

docker build -t $IMAGE_NAME docker/WasmServerless --no-cache

# rm -f $DOCKER_DIR/node_config.yaml
# rm -rf $DOCKER_DIR/target
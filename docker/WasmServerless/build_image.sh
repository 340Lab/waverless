#!/bin/bash

CURRENT_DIR=`pwd`
DOCKER_DIR="$CURRENT_DIR/docker/WasmServerless"
IMAGE_VERSION="v1"
IMAGE_NAME="wasm_serverless:$IMAGE_VERSION"

# if [ ! -d $TARGET_DIR ]
# then
#     cargo build --release
#     if test $? -ne 0
#     then
#         exit 1
#     fi
# fi
echo ">"
ls $CURRENT_DIR/target
echo ">"
ls $CURRENT_DIR/target/release
echo ">"
ls $CURRENT_DIR/apps/fn2/target
echo ">"
ls $CURRENT_DIR/apps/fn2/target/wasm32-wasi/release

rm -rf $DOCKER_DIR/target
rm -rf $DOCKER_DIR/apps

mkdir -p $DOCKER_DIR/target/release
mkdir -p $DOCKER_DIR/apps

cp $CURRENT_DIR/target/release/wasm_serverless $DOCKER_DIR/target/release/wasm_serverless
cp -r $CURRENT_DIR/apps $DOCKER_DIR

# cp "$CURRENT_DIR/node_config.yaml" $DOCKER_DIR

docker build -t $IMAGE_NAME docker/WasmServerless --no-cache

# rm -f $DOCKER_DIR/node_config.yaml
# rm -rf $DOCKER_DIR/target
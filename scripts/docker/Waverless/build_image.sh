#!/bin/bash

CURRENT_DIR=`pwd`
DOCKER_DIR="$CURRENT_DIR/scripts/docker/Waverless"
IMAGE_VERSION="v1"
IMAGE_NAME="wasm_serverless:$IMAGE_VERSION"

docker build -t $IMAGE_NAME scripts/docker/Waverless --no-cache
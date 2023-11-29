#!/bin/bash

pushd ./docker

compose="docker-compose"

${compose} up -d

${compose} down

popd
#!/bin/bash
set -e

OUTPUT_DIR="$(pwd)/dist"
RABBITMQ_VERSION="4.3.4"

mkdir -p "${OUTPUT_DIR}"
rm -rf "${OUTPUT_DIR:?}"/*

echo "Building plugin..."
docker build \
    --build-arg RABBITMQ_VERSION=${RABBITMQ_VERSION} \
    -f Dockerfile.build \
    --output type=local,dest="${OUTPUT_DIR}" \
    .

echo "Done. Plugin saved to: ${OUTPUT_DIR}"
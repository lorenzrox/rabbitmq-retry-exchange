#!/bin/bash
set -e

IMAGE="rabbitmqdevenv/build-env-27.3"
CONTAINER_NAME="build-rabbitmq-retry-exchange"
OUTPUT_DIR="$(pwd)/dist"
RABBITMQ_VERSION="v4.3.4"

mkdir -p "${OUTPUT_DIR}"
rm -rf ${OUTPUT_DIR}/*

# Cleanup function
cleanup() {
    echo "Cleaning up container..."
    docker rm -f ${CONTAINER_NAME} >/dev/null 2>&1 || true
}

trap cleanup EXIT

echo "Starting build container..."
docker run --rm -dit --name ${CONTAINER_NAME} ${IMAGE} bash

echo "Cloning RabbitMQ repo..."
docker exec ${CONTAINER_NAME} git clone --branch ${RABBITMQ_VERSION} https://github.com/rabbitmq/rabbitmq-server.git /rabbitmq-server

echo "Copying local plugin files..."
docker cp "$(pwd)" ${CONTAINER_NAME}:/rabbitmq-server/deps/rabbitmq-retry-exchange

echo "Building plugin..."
docker exec ${CONTAINER_NAME} bash -c "cd /rabbitmq-server && make RABBITMQ_VERSION=${RABBITMQ_VERSION} -C deps/rabbitmq-retry-exchange DIST_AS_EZS=yes dist"

echo "Extracting built plugin..."
docker cp ${CONTAINER_NAME}:/rabbitmq-server/deps/rabbitmq-retry-exchange/plugins/rabbitmq_retry_exchange-${RABBITMQ_VERSION}.ez "${OUTPUT_DIR}"
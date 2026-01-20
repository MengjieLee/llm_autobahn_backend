#!/usr/bin/env bash

# 一键构建镜像脚本（可传入版本号），默认 latest
# 用法：
#   ./build_image.sh            # 构建 llm_autobahn_backend:latest
#   ./build_image.sh v1.0.0     # 构建 llm_autobahn_backend:v1.0.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="llm_autobahn_backend"
IMAGE_TAG="${1:-latest}"

echo "开始构建镜像: ${IMAGE_NAME}:${IMAGE_TAG}"

docker build \
  -t "${IMAGE_NAME}:${IMAGE_TAG}" \
  "${SCRIPT_DIR}"

echo "镜像构建完成: ${IMAGE_NAME}:${IMAGE_TAG}"

mkdir -p dist && docker save -o dist/${IMAGE_NAME}-${IMAGE_TAG}.tar ${IMAGE_NAME}:${IMAGE_TAG}

echo "镜像文件已保存: dist/${IMAGE_NAME}-${IMAGE_TAG}.tar"

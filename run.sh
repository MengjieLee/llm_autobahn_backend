#!/usr/bin/env bash

set -euo pipefail

# 脚本所在目录，避免依赖工作目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

ENV_NAME="llm_autobahn_backend"

if ! command -v conda >/dev/null 2>&1; then
  echo "错误: 未找到 conda，请先安装 Anaconda 或 Miniconda，并确保其在 PATH 中。"
  exit 1
fi

echo "检查 conda 环境: ${ENV_NAME}"
if ! conda info --envs | awk '{print $1}' | grep -qx "${ENV_NAME}"; then
  echo "未找到环境 ${ENV_NAME}，正在使用 environment.yml 创建..."
  conda env create -f "${SCRIPT_DIR}/environment.yml" || {
    echo "创建 conda 环境失败，请检查 environment.yml。"
    exit 1
  }
else
  echo "已存在环境 ${ENV_NAME}，执行依赖更新..."
  conda env update -n "${ENV_NAME}" -f "${SCRIPT_DIR}/environment.yml" --prune || {
    echo "更新 conda 环境失败，请检查 environment.yml。"
    exit 1
  }
fi

echo "使用 conda run 启动 FastAPI 应用..."

# 后台启动应用（不再通过管道重定向日志，避免缓冲导致延迟）
conda run -n "${ENV_NAME}" uvicorn app.main:app --host 0.0.0.0 --port 8739 --log-level critical 2>&1 &
APP_PID=$!

echo "FastAPI 进程已启动，PID=${APP_PID}，正在等待健康检查通过..."

# 简单轮询健康检查，最多等待 30 秒
START_MSG="LLM Autobahn Backend 启动成功，监听 0.0.0.0:8739"
TIMEOUT_MSG="LLM Autobahn Backend 启动健康检查超时，请检查应用日志"

for i in {1..30}; do
  if curl -s "http://127.0.0.1:8739/health" | grep -q '"status"[[:space:]]*:[[:space:]]*"ok"'; then
    echo "${START_MSG}"
    break
  fi
  sleep 1
  # 如果进程已经退出，则不再继续等待
  if ! kill -0 "${APP_PID}" 2>/dev/null; then
    break
  fi
done

if ! kill -0 "${APP_PID}" 2>/dev/null; then
  echo "${TIMEOUT_MSG}"
fi

# 等待应用进程退出，并在退出时打印日志
wait "${APP_PID}"
EXIT_CODE=$?
STOP_MSG="LLM Autobahn Backend 已停止，退出码 ${EXIT_CODE}"
echo "${STOP_MSG}"
exit "${EXIT_CODE}"


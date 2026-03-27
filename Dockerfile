ARG TARGET_PLATFORM=linux/amd64

# ============================================================
# 构建阶段：安装编译依赖 + Python 包
# ============================================================
FROM --platform=${TARGET_PLATFORM} python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# 安装编译依赖（仅构建阶段需要）
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    pkg-config \
    libmariadb-dev \
  && rm -rf /var/lib/apt/lists/*

# 先单独 COPY requirements.txt，利用 Docker 层缓存
# 只要 requirements.txt 不变，以下 pip install 不会重跑
COPY requirements.txt .
RUN python -m venv /opt/venv \
  && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
  && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ============================================================
# 运行阶段：精简镜像
# ============================================================
FROM --platform=${TARGET_PLATFORM} python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /workspace

# 运行时系统依赖
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libmariadb3 \
    procps \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 从构建阶段复制虚拟环境
COPY --from=builder /opt/venv /opt/venv

# 先拷贝不常变的基础设施代码（利用缓存分层）
COPY src/ /workspace/src/
COPY scripts/ /workspace/scripts/
COPY context/ /workspace/context/

# 再拷贝频繁变动的应用代码
COPY app/ /workspace/app/

# 运行时数据目录（容器内创建，生产环境挂载外部卷）
RUN mkdir -p /workspace/olap_database /workspace/logs /workspace/es_logs

EXPOSE 8739

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8739"]

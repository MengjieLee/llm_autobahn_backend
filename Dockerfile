ARG TARGET_PLATFORM=linux/amd64

# ============================================================
# 构建阶段：安装编译依赖 + Python 包 + C++ 二进制
# ============================================================
FROM --platform=${TARGET_PLATFORM} python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# 安装编译依赖（Python 扩展 + C++ cache_calc）
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    pkg-config \
    libmariadb-dev \
  && rm -rf /var/lib/apt/lists/*

# 先单独 COPY requirements.txt，利用 Docker 层缓存
# 只要 requirements.txt 不变，以下 pip install 不会重跑
COPY requirements.txt .
RUN python -m venv /opt/venv \
  && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
  && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# 编译 cache_calc C++ 二进制（simulate 阶段依赖）
COPY src/domains/kv/cache_hit_rate/ /build/cache_hit_rate/
RUN cd /build/cache_hit_rate && make clean && make

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

# 从构建阶段复制编译好的 cache_calc 二进制
COPY --from=builder /build/cache_hit_rate/cache_calc /workspace/src/domains/kv/cache_hit_rate/cache_calc

# 再拷贝频繁变动的应用代码
COPY app/ /workspace/app/

# 运行时数据目录（容器内创建，生产环境挂载外部卷）
RUN mkdir -p /workspace/olap_database /workspace/logs /workspace/es_logs

EXPOSE 8739

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8739"]

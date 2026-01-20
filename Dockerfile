ARG TARGET_PLATFORM=linux/amd64

# 构建阶段：安装编译依赖
FROM --platform=${TARGET_PLATFORM} python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

# 安装编译依赖（仅构建阶段需要）
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    pkg-config \
    libmariadb-dev \
  && rm -rf /var/lib/apt/lists/*

# 创建虚拟环境并安装 Python 依赖
COPY requirements.txt /workspace/requirements.txt
RUN python -m venv /opt/venv \
  && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
  && /opt/venv/bin/pip install --no-cache-dir -r /workspace/requirements.txt

# 运行阶段：只保留运行时文件
FROM --platform=${TARGET_PLATFORM} python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /workspace

# 只安装运行时必需的系统依赖
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libmariadb3 \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

# 从构建阶段复制虚拟环境
COPY --from=builder /opt/venv /opt/venv

# 拷贝业务代码
COPY . /workspace

EXPOSE 8739

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8739"]

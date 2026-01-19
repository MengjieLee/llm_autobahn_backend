## llm_autobahn_backend

基于 FastAPI 的单体后端骨架，用于大模型垂类领域整合。

### 启动方式

- **准备 Conda 环境**
  - 确保本机已安装 Anaconda/Miniconda，并且 `conda` 在 PATH 中。

- **同步鉴权文件环境**
  - 确保本机已有 /path/to/your/workspace/llm_autobahn_backend/credentials.txt

- **一键启动（推荐）**
  - 在任意目录下执行：

```bash
bash /path/to/your/workspace/llm_autobahn_backend/run.sh
```

脚本会自动：

- 检查并创建/更新 `llm_autobahn_backend` conda 环境（基于 `environment.yml`）
- 使用 `uvicorn` 启动 FastAPI 应用

### 开发相关

- **激活你的环境**
conda activate {你的环境名,如 llm_autobahn_backend}

- **安装 py 依赖**
- conda install pyjwt -y

- **依赖更新（推荐）**
- conda env export --no-builds | grep -v "^prefix:" > environment.yml

### 主要特性

- **按领域模块划分 API**：示例领域 `domain_integration`，后续可扩展更多垂类领域模块
- **统一的请求 / 响应 / 异常 Schema**：`BaseRequest`、`StandardResponse`、`ErrorResponse`
- **规范的 Swagger 文档**：访问 `/docs` 或 `/redoc`
- **日志功能与滚动策略**：单个日志文件最大 5MB，超出后自动轮转，日志包含时间、级别、文件、行号等信息


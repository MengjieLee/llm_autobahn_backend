## llm_autobahn_backend

基于 FastAPI 的单体后端骨架，用于大模型垂类领域整合。

### 启动方式

- **同步鉴权文件环境**
  - 确保本机已有 /path/to/your/workspace/llm_autobahn_backend/credentials.txt

- **一键启动（推荐）**
  - 在任意目录下执行：

```bash
docker run -d -p 8739:8739 --name=data_autobahn --privileged \
    --hostname=localhost \
    --ulimit memlock=-1 --ulimit nofile=65536:65536 \
    -v ./.workspace_logs/:/workspace/logs/ \
    llm_autobahn_backend:0.1.0
```

### Docker 一键构建&运行

- 直接构建（默认 latest）
  ```bash
  ./build_image.sh
  ```

- 指定版本号
  ```bash
  ./build_image.sh 0.1.0
  ```

- 运行
  ```bash
  docker run -d -p 8739:8739 --name=data_autobahn --privileged \
    --hostname=localhost \
    --ulimit memlock=-1 --ulimit nofile=65536:65536 \
    -v ./.workspace_logs/:/workspace/logs/ \
    llm_autobahn_backend:0.1.0
  ```

- 开发
  ```bash
  docker save -o llm_autobahn_backend-0.1.0.tar llm_autobahn_backend:0.1.0
  ```

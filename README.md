## llm_autobahn_backend

基于 FastAPI 的单体后端骨架，用于大模型垂类领域整合。


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
  docker run --name=data_autobahn_backend --privileged \
    --hostname=localhost --network host \
    --shm-size 40G \
    --ulimit memlock=-1 --ulimit nofile=65536:65536 \
    -v /mnt/cfs_bj_mt/:/mnt/cfs_bj_mt/ \
    -v /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/:/mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/ \
    --workdir /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend \
    -it llm_autobahn_backend:latest bash
  ```

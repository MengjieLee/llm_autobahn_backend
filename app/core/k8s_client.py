"""
K8s Job 管理模块 — 创建 / 删除 / 查询 OLAP pipeline Job。

使用 kubeconfig 文件认证，Pod 配置从 olap_deployment.yml 热加载，
业务参数从 olap_config.json 读取。
"""

import logging
import os
import copy

import yaml
from kubernetes import client, config

logger = logging.getLogger(__name__)

# ============================================================
# 路径常量
# ============================================================
_CONF_DIR = os.path.join(os.path.dirname(__file__), "..", "conf")
_DEPLOYMENT_YAML = os.path.join(_CONF_DIR, "olap_deployment.yml")


def _no_proxy_api_client(kubeconfig_path: str) -> client.ApiClient:
    """创建 K8s ApiClient，绕过容器内的 HTTP_PROXY 设置"""
    config.load_kube_config(config_file=kubeconfig_path)
    configuration = client.Configuration.get_default_copy()
    configuration.proxy = None
    configuration.no_proxy = "*"
    return client.ApiClient(configuration)


def _get_batch_api(kubeconfig_path: str) -> client.BatchV1Api:
    return client.BatchV1Api(_no_proxy_api_client(kubeconfig_path))


def _load_deployment_template() -> dict:
    """热加载 olap_deployment.yml 模板，修改后下次创建 Job 即生效。"""
    with open(_DEPLOYMENT_YAML, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _make_job_name(task_id: str) -> str:
    """生成符合 K8s 命名规范的 Job 名称（≤63 字符）"""
    job_name = f"pipeline-{task_id[-48:]}" if len(task_id) > 48 else f"pipeline-{task_id}"
    return job_name.lower().replace("_", "-")[:63]


# ============================================================
# Job 创建
# ============================================================
def create_pipeline_job(
    kubeconfig_path: str,
    olap_config: dict,
    task_id: str,
    username: str,
    start_datetime: str,
    end_datetime: str,
    app_id: str,
    path: str,
    models: str = "",
) -> str:
    """
    从 olap_deployment.yml 模板创建 K8s Job。

    所有配置项直接从 olap_config 读取，无内置默认值。

    Returns:
        Job name
    """
    namespace = olap_config["namespace"]
    job_name = _make_job_name(task_id)
    mount_path = olap_config["k8s_cfs_mount_path"]

    # 命令行参数
    command = [
        "python", "scripts/run_pipeline.py",
        "--task-id", task_id,
        "--username", username,
        "--start-datetime", start_datetime,
        "--end-datetime", end_datetime,
        "--app-id", app_id,
        "--path", path,
    ]
    if models:
        command.extend(["--models", models])

    # 环境变量
    hf_cache = os.path.join(
        olap_config["k8s_working_dir"], "local_workspace", "hf_cache"
    )
    env_vars = [
        {"name": "OLAP_BASE_DIR", "value": mount_path},
        {"name": "HF_HOME", "value": hf_cache},
        {"name": "HF_TOKEN", "value": os.environ.get("HF_TOKEN", "")},
        {"name": "NO_PROXY", "value": "127.0.0.1,localhost,local,.local,172.0.0.0/24,10.0.0.0/24,.baidu.com,.baidu-int.com,iregistry.baidu-int.com,.bcebos.com,.baidubce.com,.bdbl,.bjdd,127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"},
        {"name": "no_proxy", "value": "127.0.0.1,localhost,local,.local,172.0.0.0/24,10.0.0.0/24,.baidu.com,.baidu-int.com,iregistry.baidu-int.com,.bcebos.com,.baidubce.com,.bdbl,.bjdd,127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"},
        {"name": "ALL_PROXY", "value": "http://mt:mtstudio@10.8.17.48:8777"},
        {"name": "HTTP_PROXY", "value": "http://mt:mtstudio@10.8.17.48:8777"},
        {"name": "HTTPS_PROXY", "value": "http://mt:mtstudio@10.8.17.48:8777"},
        {"name": "http_proxy", "value": "http://mt:mtstudio@10.8.17.48:8777"},
        {"name": "https_proxy", "value": "http://mt:mtstudio@10.8.17.48:8777"},
    ]

    # ---------- 加载 YAML 模板并替换变量 ----------
    tpl = _load_deployment_template()

    var_map = {
        "${JOB_NAME}": job_name,
        "${NAMESPACE}": namespace,
        "${IMAGE}": olap_config["k8s_image"],
        "${TASK_ID}": task_id[:63],
        "${MOUNT_PATH}": mount_path,
        "${WORKING_DIR}": olap_config["k8s_working_dir"],
        "${HOST_PATH}": olap_config["k8s_cfs_host_path"],
        "${CPU_REQUEST}": olap_config["k8s_job_cpu_request"],
        "${CPU_LIMIT}": olap_config["k8s_job_cpu_limit"],
        "${MEMORY_REQUEST}": olap_config["k8s_job_memory_request"],
        "${MEMORY_LIMIT}": olap_config["k8s_job_memory_limit"],
        "${TTL_SECONDS}": int(olap_config["k8s_job_ttl_seconds"]),
    }

    def _substitute(obj):
        """递归替换 dict/list 中的 ${VAR} 占位符"""
        if isinstance(obj, str):
            if obj in var_map:
                return var_map[obj]
            for k, v in var_map.items():
                if k in obj:
                    obj = obj.replace(k, str(v))
            return obj
        if isinstance(obj, dict):
            return {k: _substitute(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_substitute(item) for item in obj]
        return obj

    job_dict = _substitute(copy.deepcopy(tpl))

    # 注入动态字段（command、env）
    container = job_dict["spec"]["template"]["spec"]["containers"][0]
    container["command"] = command
    container["env"] = env_vars

    # 提交 Job
    batch_api = _get_batch_api(kubeconfig_path)
    batch_api.create_namespaced_job(namespace=namespace, body=job_dict)
    logger.info(f"[k8s] Created Job {job_name} in {namespace} for task {task_id}")
    return job_name


# ============================================================
# Job 删除
# ============================================================
def delete_pipeline_job(kubeconfig_path: str, olap_config: dict, task_id: str) -> bool:
    """删除 pipeline Job（级联删除 Pod）。"""
    namespace = olap_config["namespace"]
    job_name = _make_job_name(task_id)

    batch_api = _get_batch_api(kubeconfig_path)
    try:
        batch_api.delete_namespaced_job(
            name=job_name,
            namespace=namespace,
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        )
        logger.info(f"[k8s] Deleted Job {job_name} in {namespace}")
        return True
    except client.ApiException as e:
        if e.status == 404:
            logger.info(f"[k8s] Job {job_name} not found in {namespace}, skip delete")
            return False
        raise


# ============================================================
# Job 状态查询
# ============================================================
def get_job_status(kubeconfig_path: str, olap_config: dict, task_id: str) -> dict | None:
    """
    查询 Job + Pod 状态。

    返回字段:
      status: completed / failed / running / pending / unknown
      reason: OOMKilled / Error / DeadlineExceeded / Unschedulable / ... (失败时)
      message: 人类可读描述
    """
    namespace = olap_config["namespace"]
    job_name = _make_job_name(task_id)

    api_client = _no_proxy_api_client(kubeconfig_path)
    batch_api = client.BatchV1Api(api_client)
    core_api = client.CoreV1Api(api_client)

    try:
        job = batch_api.read_namespaced_job(name=job_name, namespace=namespace)
    except client.ApiException as e:
        if e.status == 404:
            return None
        raise

    s = job.status
    active = s.active or 0
    succeeded = s.succeeded or 0
    failed = s.failed or 0

    reason = ""
    message = ""

    # 查 Pod 状态获取更详细的信息
    try:
        pods = core_api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"job-name={job_name}",
        )
        if pods.items:
            pod = pods.items[0]
            phase = pod.status.phase  # Pending / Running / Succeeded / Failed

            # 检查容器终止原因（OOMKilled 等）
            if pod.status.container_statuses:
                cs = pod.status.container_statuses[0]
                if cs.state.terminated:
                    reason = cs.state.terminated.reason or ""
                    message = cs.state.terminated.message or ""
                elif cs.state.waiting:
                    reason = cs.state.waiting.reason or ""
                    message = cs.state.waiting.message or ""

            # Pod 还在 Pending（未被调度或等待资源）
            if phase == "Pending":
                # 检查是否因资源不足无法调度
                if pod.status.conditions:
                    for cond in pod.status.conditions:
                        if cond.type == "PodScheduled" and cond.status == "False":
                            reason = cond.reason or "Unschedulable"
                            message = cond.message or "等待集群资源"
                            break
                return {
                    "name": job_name,
                    "active": active, "succeeded": succeeded, "failed": failed,
                    "status": "pending",
                    "reason": reason or "Pending",
                    "message": message or "Pod 等待调度",
                }
    except Exception as e:
        logger.warning(f"[k8s] Failed to query Pod for {job_name}: {e}")

    if succeeded > 0:
        status_str = "completed"
    elif failed > 0:
        status_str = "failed"
        # 检查 Job 级别的失败原因（如 DeadlineExceeded）
        if not reason and s.conditions:
            for cond in s.conditions:
                if cond.type == "Failed" and cond.status == "True":
                    reason = reason or cond.reason or ""
                    message = message or cond.message or ""
    elif active > 0:
        status_str = "running"
    else:
        status_str = "unknown"

    return {
        "name": job_name,
        "active": active,
        "succeeded": succeeded,
        "failed": failed,
        "status": status_str,
        "reason": reason,
        "message": message,
    }


if __name__ == "__main__":
    import json
    cfg = json.load(open(os.path.join(_CONF_DIR, "olap_config.json")))
    api = _get_batch_api('app/conf/inner_cluster.kubeconfig')
    jobs = api.list_namespaced_job(cfg["namespace"])
    print(f'Current jobs: {len(jobs.items)}')

#!/usr/bin/env python3
"""
部署实时 Worker 到 K8s Deployment。

读取 realtime_config.json 和 realtime_deployment.yml，
替换 ${VAR} 占位符后 apply 到集群。
纯字符串替换，不依赖 pyyaml。

用法:
    python scripts/deploy_realtime.py                # 部署/更新 Deployment
    python scripts/deploy_realtime.py --delete       # 删除 Deployment
    python scripts/deploy_realtime.py --dry-run      # 仅打印替换后的 YAML，不 apply
"""

import argparse
import json
import os
import subprocess

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR = os.path.dirname(_SCRIPT_DIR)

REALTIME_CONFIG_JSON = os.path.join(_BASE_DIR, "app", "conf", "realtime_config.json")
REALTIME_DEPLOYMENT_YML = os.path.join(_BASE_DIR, "app", "conf", "realtime_deployment.yml")
KUBECONFIG_PATH = os.path.join(_BASE_DIR, "app", "conf", "inner_cluster.kubeconfig")


def _load_config() -> dict:
    with open(REALTIME_CONFIG_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="部署实时 Worker 到 K8s")
    parser.add_argument("--delete", action="store_true", help="删除 Deployment")
    parser.add_argument("--dry-run", action="store_true", help="仅打印替换后的 YAML")
    args = parser.parse_args()

    cfg = _load_config()
    namespace = cfg["k8s_namespace"]

    if args.delete:
        print(f"删除 Deployment realtime-worker (namespace={namespace})...")
        subprocess.run([
            "kubectl", "--kubeconfig", KUBECONFIG_PATH,
            "-n", namespace, "delete", "deployment", "realtime-worker",
            "--ignore-not-found",
        ], check=True)
        print("已删除")
        return

    # 加载模板文本并替换 ${VAR}
    with open(REALTIME_DEPLOYMENT_YML, "r", encoding="utf-8") as f:
        content = f.read()

    var_map = {
        "${NAMESPACE}": namespace,
        "${IMAGE}": cfg["k8s_image"],
        "${WORKING_DIR}": cfg["k8s_working_dir"],
        "${MOUNT_PATH}": cfg["k8s_cfs_mount_path"],
        "${HOST_PATH}": cfg["k8s_cfs_host_path"],
        "${CPU_REQUEST}": cfg["k8s_cpu_request"],
        "${CPU_LIMIT}": cfg["k8s_cpu_limit"],
        "${MEMORY_REQUEST}": cfg["k8s_memory_request"],
        "${MEMORY_LIMIT}": cfg["k8s_memory_limit"],
    }

    for placeholder, value in var_map.items():
        content = content.replace(placeholder, str(value))

    if args.dry_run:
        print(content)
        return

    # 写入临时文件并 apply
    tmp_yml = "/tmp/realtime_worker_deployment.yml"
    with open(tmp_yml, "w") as f:
        f.write(content)

    print(f"部署 realtime-worker (namespace={namespace}, cpu={cfg['k8s_cpu_request']}, mem={cfg['k8s_memory_request']})...")
    subprocess.run([
        "kubectl", "--kubeconfig", KUBECONFIG_PATH,
        "-n", namespace, "apply", "-f", tmp_yml,
    ], check=True)
    print("部署成功")
    os.remove(tmp_yml)


if __name__ == "__main__":
    main()

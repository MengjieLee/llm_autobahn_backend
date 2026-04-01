#!/usr/bin/env python3
"""
每日定时 Cache 理论分析计划表调度脚本

运行时机：每天凌晨 00:05:00（crontab: 5 0 * * *）
功能：从 app/conf/daily_tasks.json 热加载任务列表，批量提交 OLAP 分析任务

热更新任务：修改 local_workspace/daily_tasks.json 即可，无需重启或改代码：
    - enabled: false  → 临时禁用某个任务
    - 修改 models / app_id / 时间偏移 → 立即生效（下次 cron 执行时）
    - 新增任务条目     → 下次自动执行

用法：
    python scripts/daily_cache_plan.py                      # 直接运行
    python scripts/daily_cache_plan.py --dry-run            # 打印请求但不发送
    python scripts/daily_cache_plan.py --base-url http://...  # 指定 API 地址

crontab 配置（crontab -e 添加）：
    5 0 * * * /usr/bin/python3 /path/to/scripts/daily_cache_plan.py >> /path/to/logs/daily_cache_plan.log 2>&1
"""

import argparse
import json
import sys
import os
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone

# ============================================================
# 基础配置
# ============================================================
BJT = timezone(timedelta(hours=8))

DEFAULT_BASE_URL = "http://localhost:8739"
API_PATH = "/api/v1/olap/kv/fetch"
DEFAULT_TOKEN = os.environ.get("OLAP_API_TOKEN", "")

# daily_tasks.json 路径（相对脚本位置推导）
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR = os.path.dirname(_SCRIPT_DIR)
DEFAULT_TASKS_JSON = os.path.join(_BASE_DIR, "local_workspace", "daily_tasks.json")


# ============================================================
# 热加载任务配置
# ============================================================
def load_tasks_config(config_path: str) -> list:
    """
    从 JSON 文件热加载任务列表，每次运行都重新读取文件，修改即时生效。

    JSON 格式见 local_workspace/daily_tasks.json。
    task_name 支持 {mm-dd} 占位符，运行时替换为昨天的月日。

    time_offset 字段说明：
        day:  相对于"今天 00:00"的天偏移，-1 = 昨天，0 = 今天
        time: 具体时刻字符串，如 "00:00:00"、"20:00:00"
    """
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    tasks = cfg.get("tasks", [])
    enabled = [t for t in tasks if t.get("enabled", True)]
    return enabled


def resolve_task(task: dict, today: datetime, yesterday: datetime) -> dict:
    """
    将 task 模板中的占位符和时间偏移替换为具体值。
    """
    mm_dd = yesterday.strftime("%m-%d")

    def resolve_offset(offset: dict) -> str:
        day_offset = offset.get("day", -1)
        time_str = offset.get("time", "00:00:00")
        base = today + timedelta(days=day_offset)
        return f"{base.strftime('%Y-%m-%d')} {time_str}"

    return {
        "task_name":      task["task_name"].replace("{mm-dd}", mm_dd),
        "app_id":         task.get("app_id", ""),
        "path":           task.get("path", ""),
        "models":         task.get("models", ""),
        "start_datetime": resolve_offset(task["start_offset"]),
        "end_datetime":   resolve_offset(task["end_offset"]),
    }


# ============================================================
# HTTP 请求
# ============================================================
def submit_task(base_url: str, token: str, params: dict, dry_run: bool) -> dict:
    """
    调用 GET /api/v1/kv/fetch 提交单个任务。
    返回 {"ok": bool, "task_id": str, "message": str}
    """
    # 只过滤 None，空字符串需要原样传给后端（如 app_id=""、path="" 表示全局/全场景）
    query_params = {k: v for k, v in params.items() if v is not None}
    url = f"{base_url.rstrip('/')}{API_PATH}?{urllib.parse.urlencode(query_params)}"

    if dry_run:
        print(f"  [DRY-RUN] GET {url}")
        return {"ok": True, "task_id": "dry-run", "message": "dry-run"}

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
            task_id = body.get("data", {}).get("task_id", "")
            return {"ok": True, "task_id": task_id, "message": body.get("message", "")}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        return {"ok": False, "task_id": "", "message": f"HTTP {e.code}: {err_body[:300]}"}
    except Exception as e:
        return {"ok": False, "task_id": "", "message": str(e)}


# ============================================================
# 主逻辑
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="每日 Cache 分析计划表调度")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL,
                        help=f"API 服务地址 (默认: {DEFAULT_BASE_URL})")
    parser.add_argument("--token", default=DEFAULT_TOKEN,
                        help="API Bearer Token（也可通过环境变量 OLAP_API_TOKEN 设置）")
    parser.add_argument("--tasks-json", default=DEFAULT_TASKS_JSON,
                        help=f"任务配置文件路径 (默认: {DEFAULT_TASKS_JSON})")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅打印请求参数，不实际发送")
    args = parser.parse_args()

    now = datetime.now(BJT)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    print(f"{'='*60}")
    print(f"每日 Cache 分析计划表")
    print(f"执行时间: {now.strftime('%Y-%m-%d %H:%M:%S')} (BJT)")
    print(f"昨天: {yesterday.strftime('%Y-%m-%d')}，今天: {today.strftime('%Y-%m-%d')}")
    print(f"API:  {args.base_url}{API_PATH}")
    print(f"配置: {args.tasks_json}")
    if args.dry_run:
        print(f"模式: DRY-RUN（不发送请求）")
    print(f"{'='*60}")

    # 热加载任务配置
    try:
        task_templates = load_tasks_config(args.tasks_json)
    except FileNotFoundError:
        print(f"[ERROR] 任务配置文件不存在: {args.tasks_json}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] 任务配置文件 JSON 解析失败: {e}")
        sys.exit(1)

    tasks = [resolve_task(t, today, yesterday) for t in task_templates]
    total = len(tasks)
    print(f"加载任务: {total} 个（已过滤 enabled=false）\n")

    success = 0
    failed = 0
    results = []

    for idx, task in enumerate(tasks, 1):
        task_name = task["task_name"]
        print(f"[{idx}/{total}] {task_name}")
        print(f"  时间范围: {task['start_datetime']} ~ {task['end_datetime']}")
        print(f"  app_id:   {task['app_id'] or '(全局)'}")
        print(f"  场景:     {'coding plan' if task['path'] else 'all'}")
        print(f"  模型:     {task['models'] or '(全部)'}")

        result = submit_task(args.base_url, args.token, task, args.dry_run)
        results.append({"task_name": task_name, **result})

        if result["ok"]:
            success += 1
            print(f"  ✓ 提交成功  task_id={result['task_id']}")
        else:
            failed += 1
            print(f"  ✗ 提交失败  {result['message']}")
        print()

    print(f"{'='*60}")
    print(f"汇总: {success} 成功 / {failed} 失败 / {total} 合计")
    print(f"{'='*60}")

    summary = {
        "run_at":    now.strftime("%Y-%m-%d %H:%M:%S"),
        "yesterday": yesterday.strftime("%Y-%m-%d"),
        "config":    args.tasks_json,
        "total": total, "success": success, "failed": failed,
        "tasks": results,
    }
    print(f"\n[SUMMARY] {json.dumps(summary, ensure_ascii=False)}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

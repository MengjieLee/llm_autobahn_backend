#!/usr/bin/env python3
"""
每日定时 Cache 理论分析计划表调度脚本

运行时机：每天凌晨 00:05:00（crontab: 5 0 * * *）
功能：从 app/conf/daily_tasks.json 热加载任务列表，按 phase 分批提交 OLAP 分析任务

调度策略：
    - phase 1（长尾任务）先提交，独占全部 scroll 资源
    - 等待 phase 1 的 fetch 阶段完成后，再提交 phase 2（轻量任务）
    - 同一 phase 内的任务同时提交

热更新任务：修改 local_workspace/daily_tasks.json 即可，无需重启或改代码：
    - enabled: false  → 临时禁用某个任务
    - phase: 1/2/...  → 控制启动顺序
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
import time
import urllib.request
import urllib.parse
import urllib.error
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ============================================================
# 基础配置
# ============================================================
BJT = timezone(timedelta(hours=8))

DEFAULT_BASE_URL = "http://localhost:8739"
API_PATH = "/api/v1/olap/kv/fetch"
STATUS_API_PATH = "/api/v1/olap/kv/status"
DEFAULT_TOKEN = os.environ.get("OLAP_API_TOKEN", "")

# daily_tasks.json 路径（相对脚本位置推导）
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR = os.path.dirname(_SCRIPT_DIR)
DEFAULT_TASKS_JSON = os.path.join(_BASE_DIR, "local_workspace", "daily_tasks.json")

# phase 间等待配置
PHASE_POLL_INTERVAL = 30    # 轮询间隔（秒）
PHASE_POLL_TIMEOUT = 7200   # 最长等待时间（秒）= 2 小时


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
        "slice_minutes":  task.get("slice_minutes"),
        "phase":          task.get("phase", 1),
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
    query_params = {k: v for k, v in params.items() if v is not None and k != "phase"}
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


def poll_task_fetch_done(base_url: str, token: str, task_id: str) -> str | None:
    """
    查询任务状态，返回 fetch 阶段的 status。
    返回 "completed" / "failed" / "running" / None（查询失败）
    """
    url = f"{base_url.rstrip('/')}{STATUS_API_PATH}/{task_id}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode())
            data = body.get("data", body)
            fetch = data.get("pipeline", {}).get("stages", {}).get("fetch", {})
            return fetch.get("status", "running")
    except Exception:
        return None


def wait_for_phase_fetch_complete(
    base_url: str, token: str, task_ids: list[str], phase: int, dry_run: bool
) -> bool:
    """
    等待一批任务的 fetch 阶段全部完成。
    返回 True 表示全部完成，False 表示超时。
    """
    if dry_run or not task_ids:
        return True

    print(f"\n  等待 phase {phase} 的 {len(task_ids)} 个任务 fetch 完成...")
    pending = set(task_ids)
    t0 = time.time()

    while pending and (time.time() - t0) < PHASE_POLL_TIMEOUT:
        time.sleep(PHASE_POLL_INTERVAL)
        elapsed = int(time.time() - t0)

        for tid in list(pending):
            status = poll_task_fetch_done(base_url, token, tid)
            if status in ("completed", "failed"):
                pending.discard(tid)
                print(f"  [{elapsed}s] {tid} fetch {status} ({len(pending)} remaining)")

        if pending:
            print(f"  [{elapsed}s] 仍有 {len(pending)} 个任务 fetch 进行中...")

    if pending:
        print(f"  [WARN] 等待超时 ({PHASE_POLL_TIMEOUT}s)，{len(pending)} 个任务 fetch 未完成: {pending}")
        print(f"  继续提交下一阶段（利用余额资源）...")
        return False

    elapsed = int(time.time() - t0)
    print(f"  phase {phase} 全部 fetch 完成，耗时 {elapsed}s\n")
    return True


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

    # 按 phase 分组
    phase_groups = defaultdict(list)
    for task in tasks:
        phase_groups[task["phase"]].append(task)
    phases = sorted(phase_groups.keys())

    print(f"加载任务: {total} 个（已过滤 enabled=false）")
    print(f"分阶段: {', '.join(f'phase {p}: {len(phase_groups[p])} 个' for p in phases)}\n")

    success = 0
    failed = 0
    results = []
    global_idx = 0

    for phase_idx, phase in enumerate(phases):
        group = phase_groups[phase]
        print(f"{'─'*60}")
        print(f"Phase {phase}: 提交 {len(group)} 个任务")
        print(f"{'─'*60}")

        phase_task_ids = []

        for task in group:
            global_idx += 1
            task_name = task["task_name"]
            print(f"\n[{global_idx}/{total}] {task_name}  (phase {phase})")
            print(f"  时间范围: {task['start_datetime']} ~ {task['end_datetime']}")
            print(f"  app_id:   {task['app_id'] or '(全局)'}")
            print(f"  场景:     {'coding plan' if task['path'] else 'all'}")
            print(f"  模型:     {task['models'] or '(全部)'}")
            if task.get("slice_minutes") is not None:
                print(f"  子切片:   {task['slice_minutes']} 分钟")

            result = submit_task(args.base_url, args.token, task, args.dry_run)
            results.append({"task_name": task_name, "phase": phase, **result})

            if result["ok"]:
                success += 1
                print(f"  ✓ 提交成功  task_id={result['task_id']}")
                if result["task_id"] and result["task_id"] != "dry-run":
                    phase_task_ids.append(result["task_id"])
            else:
                failed += 1
                print(f"  ✗ 提交失败  {result['message']}")

        # 如果不是最后一个 phase，等待当前 phase fetch 完成
        if phase_idx < len(phases) - 1 and phase_task_ids:
            wait_for_phase_fetch_complete(
                args.base_url, args.token, phase_task_ids, phase, args.dry_run
            )

    print(f"\n{'='*60}")
    print(f"汇总: {success} 成功 / {failed} 失败 / {total} 合计")
    print(f"{'='*60}")

    summary = {
        "run_at":    now.strftime("%Y-%m-%d %H:%M:%S"),
        "yesterday": yesterday.strftime("%Y-%m-%d"),
        "config":    args.tasks_json,
        "total": total, "success": success, "failed": failed,
        "phases": {str(p): len(phase_groups[p]) for p in phases},
        "tasks": results,
    }
    print(f"\n[SUMMARY] {json.dumps(summary, ensure_ascii=False)}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

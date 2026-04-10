#!/usr/bin/env python3
"""
实时 Cache 分析调度器

每分钟 cron 执行，为"全场景_各模型"场景生成 1 分钟窗口的任务文件，
写入 CFS 队列目录，由常驻 Worker Pod 消费处理。

设计要点：
- 幂等：多级检查（queue/pending、queue/running、每日 JSON）防止重复提交
- 轻量：纯文件操作，不启动 K8s Job
- 5 分钟延迟：确保 ES 数据已完整写入

用法:
    python scripts/realtime_scheduler.py                      # 正常执行
    python scripts/realtime_scheduler.py --dry-run            # 仅打印，不写文件
    python scripts/realtime_scheduler.py --delay-minutes 5   # 自定义延迟

crontab 配置:
    * * * * * /usr/bin/python3 /path/to/scripts/realtime_scheduler.py >> /path/to/logs/realtime/realtime_scheduler_$(date +\%Y-\%m-\%d).log 2>&1
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ============================================================
# 路径 & 配置
# ============================================================
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR = os.path.dirname(_SCRIPT_DIR)

OLAP_DATABASE_DIR = os.path.join(_BASE_DIR, "olap_database")
REALTIME_DIR = os.path.join(OLAP_DATABASE_DIR, "realtime")
QUEUE_PENDING_DIR = os.path.join(REALTIME_DIR, "queue", "pending")
QUEUE_RUNNING_DIR = os.path.join(REALTIME_DIR, "queue", "running")
QUEUE_DONE_DIR = os.path.join(REALTIME_DIR, "queue", "done")
QUEUE_FAILED_DIR = os.path.join(REALTIME_DIR, "queue", "failed")
REALTIME_CONFIG_JSON = os.path.join(_BASE_DIR, "app", "conf", "realtime_config.json")

BJT = timezone(timedelta(hours=8))


def _now_bjt() -> str:
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")


def _load_realtime_config() -> dict:
    try:
        with open(REALTIME_CONFIG_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _load_daily_data(date_str: str, scenario: str) -> dict:
    """读取每日 JSON 文件的 data dict"""
    daily_file = os.path.join(REALTIME_DIR, scenario, f"{date_str}.json")
    if not os.path.exists(daily_file):
        return {}
    try:
        with open(daily_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("data", {})
    except Exception:
        return {}


def _minute_exists_in_queue(date_str: str, minute_str: str) -> bool:
    """检查 queue/pending/ 或 queue/running/ 是否已有该分钟的任务"""
    # 文件名格式: 2026-04-10_11-38.json (minute 中:替换为-)
    prefix_safe = f"{date_str}_{minute_str.replace(':', '-')}"
    prefix_legacy = f"{date_str}_{minute_str}"
    for queue_dir in [QUEUE_PENDING_DIR, QUEUE_RUNNING_DIR]:
        if os.path.exists(queue_dir):
            for f in os.listdir(queue_dir):
                if (f.startswith(prefix_safe) or f.startswith(prefix_legacy)) and f.endswith(".json"):
                    return True
    return False


def _write_task(task: dict):
    """写入任务文件到 queue/pending/"""
    os.makedirs(QUEUE_PENDING_DIR, exist_ok=True)
    filename = f"{task['date']}_{task['minute'].replace(':', '-')}.json"
    filepath = os.path.join(QUEUE_PENDING_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(task, f, ensure_ascii=False, indent=2)
    return filepath


def _cleanup_old_files(retention_days: int = 30):
    """清理过期的每日 JSON 文件和已完成/失败的任务文件"""
    now = datetime.now(BJT)
    scenario = _load_realtime_config().get("scenario", "全场景_各模型")
    scenario_dir = os.path.join(REALTIME_DIR, scenario)

    # 清理超过 retention_days 的每日文件
    if os.path.isdir(scenario_dir):
        cutoff_date = (now - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        for f in os.listdir(scenario_dir):
            if not f.endswith(".json"):
                continue
            if f[:-5] < cutoff_date:
                fpath = os.path.join(scenario_dir, f)
                try:
                    os.remove(fpath)
                    print(f"  [cleanup] 删除过期数据: {fpath}")
                except OSError:
                    pass

    # 清理超过 1 天的 done/ 和 failed/ 任务文件
    done_cutoff = time.time() - 86400  # 1 天
    for queue_dir in [QUEUE_DONE_DIR, QUEUE_FAILED_DIR]:
        if not os.path.isdir(queue_dir):
            continue
        for f in os.listdir(queue_dir):
            fpath = os.path.join(queue_dir, f)
            try:
                if os.path.getmtime(fpath) < done_cutoff:
                    os.remove(fpath)
            except OSError:
                pass


def main():
    parser = argparse.ArgumentParser(description="实时 Cache 分析调度器")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅打印，不写文件")
    parser.add_argument("--delay-minutes", type=int, default=5,
                        help="ES 数据延迟分钟数（默认 5）")
    args = parser.parse_args()

    cfg = _load_realtime_config()
    if not cfg.get("enabled", True):
        print(f"[skip] enabled=false")
        return

    scenario = cfg.get("scenario", "全场景_各模型")
    models_str = cfg.get("models", "glm-5,kimi-k2.5,minimax-m2.5,deepseek-v3.2")
    models = [m.strip() for m in models_str.split(",") if m.strip()]
    delay_minutes = cfg.get("delay_minutes", args.delay_minutes)
    retention_days = cfg.get("retention_days", 30)

    now = datetime.now(BJT)
    # 目标分钟: now - delay_minutes，对齐到整分钟
    target = (now - timedelta(minutes=delay_minutes)).replace(second=0, microsecond=0)
    date_str = target.strftime("%Y-%m-%d")
    minute_str = target.strftime("%H:%M")
    task_filename = f"{date_str}_{minute_str.replace(':', '-')}"

    print(f"[{_now_bjt()}] 目标: {date_str} {minute_str} (delay={delay_minutes}min)")

    # ---- 幂等检查 ----
    # 1. 检查 queue
    if _minute_exists_in_queue(date_str, minute_str):
        print(f"  [skip] {task_filename} 已在队列中")
        return
    daily_data = _load_daily_data(date_str, scenario)
    if minute_str in daily_data:
        print(f"  [skip] {task_filename} 已完成")
        return

    # ---- 构造任务 ----
    task_id = f"rt-{target.strftime('%Y%m%d-%H%M%S')}"
    task = {
        "task_id": task_id,
        "date": date_str,
        "minute": minute_str,
        "start_datetime": target.strftime("%Y-%m-%d %H:%M:%S"),
        "end_datetime": (target + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "app_id": "",
        "path": "",
        "models": models,
        "submitted_at": _now_bjt(),
    }

    if args.dry_run:
        print(f"  [DRY-RUN] {task_filename}")
        print(f"  task_id: {task_id}")
        print(f"  range: {task['start_datetime']} ~ {task['end_datetime']}")
        print(f"  models: {models}")
        return

    # ---- 写入队列 ----
    filepath = _write_task(task)
    print(f"  [ok] 写入: {filepath}")

    # ---- 清理 ----
    _cleanup_old_files(retention_days)


if __name__ == "__main__":
    main()

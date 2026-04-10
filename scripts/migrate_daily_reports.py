#!/usr/bin/env python3
"""
迁移脚本：将历史已完成任务数据写入 daily_reports/ 目录

用法：
    python scripts/migrate_daily_reports.py                # 默认处理所有日期
    python scripts/migrate_daily_reports.py --date 04-08   # 只处理指定日期
    python scripts/migrate_daily_reports.py --date 04-01,04-02,04-03  # 多日期
    python scripts/migrate_daily_reports.py --dry-run      # 只打印，不写入
"""

import argparse
import json
import re
import sys
import os
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
STATUS_DIR = BASE_DIR / "olap_database" / "status"
DAILY_REPORTS_DIR = BASE_DIR / "olap_database" / "daily_reports"


def extract_scenario_name(task_name: str, target_date: str):
    """
    从 task_name 提取场景名。
    "04-08_全场景_各模型" → "全场景_各模型"
    "【v9】04-0804-08_全场景_各模型" → "全场景_各模型"
    "【并行deamon测试_v2】04-0504-05_全场景_各模型" → "全场景_各模型"
    "04-08_无问芯穹_全场景_glm-5" → "无问芯穹_全场景_glm-5"
    """
    if target_date not in task_name:
        return None

    # 去掉 【xxx】 前缀
    name = re.sub(r"^【[^】]+】", "", task_name)

    # 去掉日期前缀，可能出现的格式:
    #   "04-08_xxx"          → "xxx"
    #   "0408_xxx"           → "xxx"
    #   "04-080408_xxx"      → "xxx"  (04-08+04-08)
    #   "04-0804-08_xxx"     → "xxx"  (04-08+04-08, 中间有横杠)
    target_mm, target_dd = target_date.split("-")
    # 尝试多种日期前缀格式
    patterns = [
        rf"^{re.escape(target_date)}_(.+)",                         # 04-08_xxx
        rf"^{target_mm}{target_dd}_(.+)",                           # 0408_xxx
        rf"^{re.escape(target_date)}{target_mm}{target_dd}_(.+)",   # 04-080408_xxx
        rf"^{re.escape(target_date)}{re.escape(target_date)}_(.+)", # 04-0804-08_xxx
    ]
    for pat in patterns:
        m = re.match(pat, name)
        if m:
            name = m.group(1)
            break
    else:
        return None

    return name if name else None


def collect_tasks(target_date: str):
    """收集指定日期的所有已完成任务"""
    tasks = []
    for status_file in STATUS_DIR.glob(f"**/*.json"):
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            task_name = data.get("task_name", "")
            if target_date not in task_name:
                continue
            if data.get("pipeline", {}).get("current_stage") != "done":
                continue
            if not data.get("result") or not isinstance(data["result"], dict):
                continue
            # 只保留来自 daily_tasks.json 的任务（以 MM-DD_ 开头或带 【】前缀）
            scenario = extract_scenario_name(task_name, target_date)
            if not scenario:
                continue
            tasks.append(data)
        except Exception:
            continue
    tasks.sort(key=lambda t: t.get("updated_at", ""), reverse=True)
    return tasks


def write_daily_report(date_label: str, tasks: list, dry_run: bool = False):
    """将任务数据写入 daily_reports/{date_label}.json"""
    DAILY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    filepath = DAILY_REPORTS_DIR / f"{date_label}.json"

    existing = {}
    if filepath.exists():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    scenarios = existing.get("scenarios", {})
    written = 0

    for task in tasks:
        task_name = task.get("task_name", "")
        result = task.get("result", {})
        if not result:
            continue

        scenario = extract_scenario_name(task_name, date_label)
        if not scenario:
            continue
        if scenario in scenarios:
            continue

        total_hit = sum(r.get("hit_count", 0) for r in result.values())
        total_queries = sum(r.get("total_queries", 0) for r in result.values())
        total_tokens = sum(r.get("total_tokens", 0) for r in result.values())
        hit_rate_pct = round((total_hit / total_queries * 100), 2) if total_queries > 0 else 0

        scenarios[scenario] = {
            "hit_rate_percent": hit_rate_pct,
            "hit_count": total_hit,
            "total_queries": total_queries,
            "total_tokens": total_tokens,
            "models": {
                model: {
                    "hit_rate_percent": stats.get("hit_rate_percent", 0),
                    "hit_count": stats.get("hit_count", 0),
                    "total_queries": stats.get("total_queries", 0),
                    "total_tokens": stats.get("total_tokens", 0),
                }
                for model, stats in result.items()
            },
            "updated_at": task.get("updated_at", ""),
        }
        written += 1
        print(f"  + {scenario}: {hit_rate_pct}%")

    if written > 0 and not dry_run:
        existing.update({
            "date": date_label,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scenarios": scenarios,
        })
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f"  → 写入 {filepath}，新增 {written} 个场景")
    elif written > 0:
        print(f"  [DRY-RUN] 将写入 {filepath}，新增 {written} 个场景")
    else:
        print(f"  (无新增场景，跳过)")

    return written


def discover_dates():
    """从 status 目录发现所有有效日期"""
    dates = set()
    for status_file in STATUS_DIR.glob(f"**/*.json"):
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            task_name = data.get("task_name", "")
            if data.get("pipeline", {}).get("current_stage") != "done":
                continue
            if not data.get("result") or not isinstance(data["result"], dict):
                continue
            # 提取日期: 匹配 "MM-DD" 格式（数字 01-12 和 01-31）
            for m_iter in re.finditer(r"(?<!\d)(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(?!\d)", task_name):
                dates.add(m_iter.group(0))
        except Exception:
            continue
    return sorted(dates, reverse=True)


def main():
    parser = argparse.ArgumentParser(description="迁移历史任务数据到 daily_reports/")
    parser.add_argument("--date", help="指定日期，如 04-08 或 04-01,04-02,04-03")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写入")
    args = parser.parse_args()

    print(f"{'='*60}")
    print(f"迁移历史日报数据")
    print(f"输出目录: {DAILY_REPORTS_DIR}")
    if args.dry_run:
        print(f"模式: DRY-RUN（不写入文件）")
    print(f"{'='*60}\n")

    if args.date:
        dates = [d.strip() for d in args.date.split(",")]
    else:
        dates = discover_dates()
        print(f"自动发现 {len(dates)} 个有数据的日期\n")

    total_written = 0
    for date_label in dates:
        print(f"处理 {date_label}...")
        tasks = collect_tasks(date_label)
        if not tasks:
            print(f"  (无已完成任务)\n")
            continue
        print(f"  找到 {len(tasks)} 个已完成任务")
        written = write_daily_report(date_label, tasks, args.dry_run)
        total_written += written
        print()

    print(f"{'='*60}")
    print(f"完成！共写入 {total_written} 个场景")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

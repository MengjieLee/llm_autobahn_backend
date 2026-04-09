#!/usr/bin/env python3
"""
清理 is_deleted=true 的任务数据目录

用法：
    python scripts/cleanup_deleted.py              # dry-run，仅打印待删除目录
    python scripts/cleanup_deleted.py --confirm     # 实际删除
"""

import argparse
import json
import os
import shutil
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR = os.path.dirname(_SCRIPT_DIR)
STATUS_DIR = os.path.join(_BASE_DIR, "olap_database", "status")
DATA_DIR = os.path.join(_BASE_DIR, "olap_database", "data")


def main():
    parser = argparse.ArgumentParser(description="清理 is_deleted=true 的任务数据")
    parser.add_argument("--confirm", action="store_true", help="实际执行删除（默认 dry-run）")
    args = parser.parse_args()

    deleted_tasks = []

    for username in sorted(os.listdir(STATUS_DIR)):
        user_status_dir = os.path.join(STATUS_DIR, username)
        if not os.path.isdir(user_status_dir):
            continue
        for fname in sorted(os.listdir(user_status_dir)):
            if not fname.endswith(".json"):
                continue
            status_file = os.path.join(user_status_dir, fname)
            try:
                with open(status_file, "r", encoding="utf-8") as f:
                    status = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            if not status.get("is_deleted"):
                continue

            task_id = status.get("task_id", fname[:-5])
            data_path = os.path.join(DATA_DIR, username, task_id)
            deleted_tasks.append((username, task_id, status_file, data_path))

    if not deleted_tasks:
        print("没有找到 is_deleted=true 的任务。")
        return

    print(f"找到 {len(deleted_tasks)} 个待清理任务：\n")
    total_size = 0
    for username, task_id, status_file, data_path in deleted_tasks:
        dir_size = 0
        has_data = os.path.isdir(data_path)
        if has_data:
            dir_size = sum(
                os.path.getsize(os.path.join(dp, f))
                for dp, _, fnames in os.walk(data_path)
                for f in fnames
            )
        total_size += dir_size
        size_str = f"{dir_size / (1024**3):.2f} GB" if dir_size > 1024**3 else f"{dir_size / (1024**2):.1f} MB"
        data_tag = size_str if has_data else "无 data"
        print(f"  [{username}] {task_id}  ({data_tag})")

    total_str = f"{total_size / (1024**3):.2f} GB" if total_size > 1024**3 else f"{total_size / (1024**2):.1f} MB"
    print(f"\n合计: {total_str}")

    if not args.confirm:
        print("\n[DRY-RUN] 以上目录未删除。添加 --confirm 实际执行。")
        return

    print()
    success = 0
    for username, task_id, status_file, data_path in deleted_tasks:
        try:
            if os.path.isdir(data_path):
                shutil.rmtree(data_path)
            os.remove(status_file)
            print(f"  ✓ 已删除 {task_id}")
            success += 1
        except Exception as e:
            print(f"  ✗ 删除失败 {task_id}: {e}")

    print(f"\n完成: {success}/{len(deleted_tasks)} 个目录已删除")


if __name__ == "__main__":
    main()

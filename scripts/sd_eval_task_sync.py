#!/usr/bin/env python3
"""
定时刷新 prepared 状态的 MTP 评测任务。
crontab: */5 * * * * cd /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend && /usr/bin/python3 scripts/sd_eval_task_sync.py >> logs/sd/sd_eval_task_sync.log 2>&1
"""

import json
import os
import sys
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

API_BASE = os.environ.get("AUTOBAHN_API_BASE", "http://127.0.0.1:8739")
MAX_WORKERS = int(os.environ.get("SYNC_MAX_WORKERS", "12"))
LIST_URL = f"{API_BASE}/api/v1/mtp_eval/tasks?poll=1"
TASK_URL_TPL = f"{API_BASE}/api/v1/mtp_eval/tasks/{{task_id}}?poll=1"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def fetch_json(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        log(f"  ERROR {url}: {e}")
        return None


def main() -> None:
    result = fetch_json(LIST_URL)
    if not result or result.get("code") != 0:
        log("Failed to fetch task list.")
        sys.exit(1)

    tasks = result.get("data") or []
    now = datetime.now()
    prepared = []
    for t in tasks:
        if t.get("status") != "prepared":
            continue
        created = t.get("created_at")
        if not created:
            continue
        try:
            ct = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue
        if (now - ct).total_seconds() <= 86400:
            prepared.append(t)

    if not prepared:
        return

    log(f"Refreshing {len(prepared)} prepared task(s)...")

    def refresh_one(task_id: str) -> str:
        resp = fetch_json(TASK_URL_TPL.format(task_id=task_id))
        status = resp.get("data", {}).get("status", "?") if resp and resp.get("code") == 0 else "failed"
        return f"  {task_id}: {status}"

    task_ids = [t["id"] for t in prepared if t.get("id")]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for future in as_completed({pool.submit(refresh_one, tid): tid for tid in task_ids}):
            log(future.result())

    log("Done.")


if __name__ == "__main__":
    main()

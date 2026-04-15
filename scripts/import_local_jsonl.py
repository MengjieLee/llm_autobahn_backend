#!/usr/bin/env python3
"""
将本地下载的请求 body JSONL 导入 ad-hoc pipeline。

用法:
    python3 scripts/import_local_jsonl.py \
        --input  "glm-5-2026-04-08-11:00-11:30-1-200k-0.5/glm-5-2026-04-08-11:00-11:30-1-200k-0.5.jsonl" \
        --model  glm-5 \
        --start  "2026-04-08 11:00:00" \
        --end    "2026-04-08 11:30:00" \
        [--username v_limengjie03] \
        [--cache-size 200000000] \
        [--block-size 16] \
        [--slice-minutes 1] \
        [--run]           # 加 --run 会自动调用 run_pipeline.py

工作流:
1. 读取每条请求 body，包装成 ES 格式 (_source.@raw)
2. 按分钟拆分成 per-minute JSONL 文件
3. 创建 status.json，将 fetch 阶段标记为已完成
4. (可选) 自动调用 run_pipeline.py 从 tokenize 阶段继续
"""

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

BJT = timezone(timedelta(hours=8))

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(_SCRIPT_DIR)
OLAP_DATABASE_DIR = os.path.join(BASE_DIR, "olap_database")
KV_DATA_DIR = os.path.join(OLAP_DATABASE_DIR, "data")
KV_STATUS_DIR = os.path.join(OLAP_DATABASE_DIR, "status")


def _now_bjt() -> str:
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")


def _make_task_id(username: str, start_dt: datetime, end_dt: datetime) -> str:
    s = start_dt.strftime("%Y%m%d_%H%M%S")
    e = end_dt.strftime("%Y%m%d_%H%M%S")
    short_hash = hashlib.md5(uuid.uuid4().bytes).hexdigest()[:8]
    return f"{username}-kv_{s}_{e}_{short_hash}"


def _wrap_to_es_format(body: dict, model: str, idx: int, ts_str: str) -> dict:
    """将原始请求 body 包装成 ES 索引记录格式"""
    body_json = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    raw_line = (
        f"[{ts_str}] [INFO] (/import/local.go:1) [parseRequestV2] "
        f"AS received a new chat v2 request, "
        f"sessionid:as-import{idx:08d}, appid:app-LOCAL, "
        f"qianfan_model:{model}, ep:, model:{model}, "
        f"path:/v2/coding/chat/completions, "
        f"body:{body_json}, rawBodyLength:{len(body_json)}"
    )
    return {
        "_index": "as-qianfan-online_import",
        "_type": "_doc",
        "_id": f"import-{idx:08d}",
        "_score": None,
        "_source": {
            "@timestamp": ts_str,
            "@raw": raw_line,
            "as_id": f"as-import{idx:08d}",
            "model": model,
            "path": "/v2/coding/chat/completions",
        },
        "sort": [idx],
    }


def main():
    parser = argparse.ArgumentParser(description="导入本地 JSONL 到 ad-hoc pipeline")
    parser.add_argument("--input", required=True, help="输入 JSONL 文件路径")
    parser.add_argument("--model", required=True, help="目标模型名，如 glm-5")
    parser.add_argument("--start", required=True, help="数据起始时间，如 '2026-04-08 11:00:00'")
    parser.add_argument("--end", required=True, help="数据结束时间，如 '2026-04-08 11:30:00'")
    parser.add_argument("--username", default="v_limengjie03", help="用户名")
    parser.add_argument("--cache-size", type=int, default=200000000, help="cache_size (tokens)")
    parser.add_argument("--block-size", type=int, default=64, help="block_size")
    parser.add_argument("--slice-minutes", type=int, default=1, help="子切片粒度（分钟）")
    parser.add_argument("--run", action="store_true", help="自动调用 run_pipeline.py 执行后续阶段")
    args = parser.parse_args()

    input_path = os.path.abspath(args.input)
    if not os.path.exists(input_path):
        print(f"[ERROR] 文件不存在: {input_path}")
        sys.exit(1)

    start_dt = datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJT)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJT)
    total_minutes = int((end_dt - start_dt).total_seconds() / 60)
    if total_minutes <= 0:
        print(f"[ERROR] 时间范围无效: {args.start} ~ {args.end}")
        sys.exit(1)

    task_id = _make_task_id(args.username, start_dt, end_dt)
    task_data_dir = os.path.join(KV_DATA_DIR, args.username, task_id)
    os.makedirs(task_data_dir, exist_ok=True)

    print(f"Task ID:    {task_id}")
    print(f"数据目录:   {task_data_dir}")
    print(f"时间范围:   {args.start} ~ {args.end} ({total_minutes} 分钟)")
    print(f"模型:       {args.model}")

    # ----------------------------------------------------------------
    # Step 1: 读取 JSONL，包装成 ES 格式，按分钟拆分写入
    # ----------------------------------------------------------------
    print("\n[Step 1] 读取 JSONL 并按分钟拆分...")

    # 统计总行数
    total_lines = 0
    with open(input_path, "r", encoding="utf-8") as f:
        for _ in f:
            total_lines += 1
    print(f"  总行数: {total_lines}")

    # 计算每分钟应分配的行数（均匀分配，因为原始数据没有时间戳）
    lines_per_minute = total_lines / total_minutes

    # 预计算每个分钟的时间信息和输出文件路径
    minute_meta = []  # [(file_path, ts_str)]
    for mi in range(total_minutes):
        minute_dt = start_dt + timedelta(minutes=mi)
        hour_dir = os.path.join(task_data_dir, minute_dt.strftime("%H"))
        os.makedirs(hour_dir, exist_ok=True)
        minute_end = minute_dt + timedelta(minutes=1)
        file_name = f"kv_{minute_dt.strftime('%Y%m%d_%H%M%S')}_{minute_end.strftime('%Y%m%d_%H%M%S')}.jsonl"
        file_path = os.path.join(hour_dir, file_name)
        ts_str = minute_dt.strftime("%Y-%m-%dT%H:%M:%S.000+08:00")
        minute_meta.append((file_path, ts_str))

    # 按分钟拆分 — 避免 json.loads 解析 body，直接字符串拼接包装
    minute_files = {}  # file_path -> file handle
    minute_counts = {}
    written = 0
    model = args.model

    with open(input_path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            body_str = line.rstrip("\n\r")
            if not body_str:
                continue

            minute_idx = min(int(idx / lines_per_minute), total_minutes - 1)
            file_path, ts_str = minute_meta[minute_idx]

            # 构造 @raw 字符串，需要 json.dumps 转义内部引号
            raw_value = (
                f"[{ts_str}] [INFO] qianfan_model:{model}, "
                f"body:{body_str}, rawBodyLength:{len(body_str)}"
            )
            # json.dumps(raw_value) 会加上外层引号并转义内部双引号
            raw_escaped = json.dumps(raw_value, ensure_ascii=False)
            es_line = (
                f'{{"_index":"as-qianfan-online_import","_type":"_doc",'
                f'"_id":"import-{idx:08d}","_score":null,'
                f'"_source":{{"@timestamp":"{ts_str}",'
                f'"@raw":{raw_escaped},'
                f'"as_id":"as-import{idx:08d}","model":"{model}",'
                f'"path":"/v2/coding/chat/completions"}},'
                f'"sort":[{idx}]}}'
            )

            if file_path not in minute_files:
                minute_files[file_path] = open(file_path, "w", encoding="utf-8", buffering=4 * 1024 * 1024)
                minute_counts[file_path] = 0

            minute_files[file_path].write(es_line + "\n")
            minute_counts[file_path] += 1
            written += 1

            if idx % 10000 == 0:
                print(f"  进度: {idx}/{total_lines} ({idx*100//total_lines}%)", flush=True)

    # 关闭所有文件
    for fh in minute_files.values():
        fh.close()

    print(f"  写入 {written} 条记录到 {len(minute_files)} 个 per-minute 文件")
    for fp, cnt in sorted(minute_counts.items()):
        print(f"    {os.path.basename(fp)}: {cnt} 条")

    # ----------------------------------------------------------------
    # Step 2: 创建 status.json，标记 fetch 已完成
    # ----------------------------------------------------------------
    print("\n[Step 2] 创建 status.json...")

    status_dir = os.path.join(KV_STATUS_DIR, args.username)
    os.makedirs(status_dir, exist_ok=True)
    status_file = os.path.join(status_dir, f"{task_id}.json")

    s_start = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    s_end = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    label = f"{start_dt.strftime('%m-%d_%H')}~{end_dt.strftime('%H')}_{args.model}_local"

    status = {
        "task_id": task_id,
        "task_name": label,
        "created_by": {"username": args.username, "name": "local_import"},
        "created_at": _now_bjt(),
        "updated_at": _now_bjt(),
        "query": {
            "start_datetime": s_start,
            "end_datetime": s_end,
            "app_id": "",
            "models": [args.model],
        },
        "scenario": {"path": "", "label": "all"},
        "pipeline": {
            "current_stage": "tokenize",
            "stages": {
                "fetch": {
                    "status": "completed",
                    "message": f"本地导入 {written} 条记录",
                    "total_count": written,
                    "total_files": len(minute_files),
                    "started_at": _now_bjt(),
                    "completed_at": _now_bjt(),
                },
                "tokenize": {"status": "pending", "message": "等待数据..."},
                "simulate": {"status": "pending", "message": ""},
                "trend": {"status": "pending", "message": ""},
            },
        },
        "config": {
            "default_model": args.model,
            "block_size": args.block_size,
            "cache_size": args.cache_size,
            "slice_minutes": args.slice_minutes,
        },
        "is_deleted": False,
        "scheduled_at": None,
        "notified": False,
    }

    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    print(f"  Status: {status_file}")

    # ----------------------------------------------------------------
    # Step 3: 输出执行命令 或 自动执行
    # ----------------------------------------------------------------
    run_cmd = (
        f"python3 {os.path.join(BASE_DIR, 'scripts', 'run_pipeline.py')} "
        f"--task-id '{task_id}' "
        f"--username '{args.username}' "
        f"--start-datetime '{s_start}' "
        f"--end-datetime '{s_end}' "
        f"--models '{args.model}' "
        f"--slice-minutes {args.slice_minutes}"
    )

    if args.run:
        print(f"\n[Step 3] 自动执行 pipeline...")
        print(f"  CMD: {run_cmd}")
        os.execv(sys.executable, [sys.executable] + run_cmd.split()[1:])
    else:
        print(f"\n[Step 3] 数据已就绪，执行以下命令启动 pipeline:")
        print(f"\n  {run_cmd}\n")
        print(f"  pipeline 会跳过 fetch 阶段，从 tokenize 开始。")


if __name__ == "__main__":
    main()

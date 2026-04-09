#!/usr/bin/env python3
"""
计算分钟级命中率趋势并生成 hit_rate_trend.json

核心逻辑：对每个模型的每个时间片 input_ids 文件调用 cache_calc，
提取 hit_rate 后按时间排序，并计算整体维度和 mean/max/min 统计。

两种使用方式:
  1) 作为库函数被 pipeline 调用:
       from scripts.compute_trend import compute_trend
       compute_trend(task_data_dir, cache_calc_path, cache_size, block_size, model_outputs)

  2) 命令行回填已完成任务:
       python scripts/compute_trend.py --status-dir olap_database/status --data-dir olap_database/data
"""

import argparse
import concurrent.futures
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

logger = logging.getLogger("compute_trend")

# ============================================================
# 路径
# ============================================================
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR = os.path.dirname(_SCRIPT_DIR)
_DEFAULT_CACHE_CALC = os.path.join(_BASE_DIR, "src/domains/kv/cache_hit_rate/cache_calc")
_DEFAULT_OLAP_CONFIG = os.path.join(_BASE_DIR, "app", "conf", "olap_config.json")

BJT = timezone(timedelta(hours=8))


def _load_olap_config() -> dict:
    try:
        with open(_DEFAULT_OLAP_CONFIG, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _parse_time_from_filename(fname: str) -> str:
    """从文件名提取起始时间: kv_YYYYMMDD_HHMMSS_... -> MM-DD HH:mm"""
    m = re.match(r'kv_(\d{8})_(\d{6})_', fname)
    if m:
        d, t = m.group(1), m.group(2)
        return f"{d[4:6]}-{d[6:8]} {t[0:2]}:{t[2:4]}"
    return fname[:16]


def _calc_single_file(txt_file: str, cache_calc_path: str, cache_size: int, block_size: int) -> dict:
    """同步调用 cache_calc 对单个 slice 文件计算命中率"""
    fname = os.path.basename(txt_file)
    time_label = _parse_time_from_filename(fname)
    t0 = _time.monotonic()
    try:
        cmd = [
            cache_calc_path, "-f", txt_file,
            "-s", str(cache_size),
            "-b", str(block_size),
            "-p", "true"
        ]
        # 动态超时：基础120秒 + 每100MB增加1秒，最大600秒（10分钟）
        file_size_mb = os.path.getsize(txt_file) / (1024 * 1024)
        timeout = min(600, max(120, int(file_size_mb / 100) + 120))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        elapsed = _time.monotonic() - t0
        if result.returncode != 0:
            logger.warning(
                "[trend] cache_calc failed for %s (rc=%d, %.1fs): %s",
                fname, result.returncode, elapsed,
                (result.stderr or result.stdout or "")[:200],
            )
            return {"time": time_label, "hit_rate": None}
        for line in result.stdout.strip().split("\n"):
            if line.strip().startswith("cache_size:"):
                parts = re.findall(r'hit_rate:\s*([\d.]+)', line)
                if parts:
                    hit_rate = float(parts[0])
                    logger.debug("[trend] %s -> hit_rate=%.4f (%.1fs)", fname, hit_rate, elapsed)
                    return {"time": time_label, "hit_rate": hit_rate}
        logger.warning(
            "[trend] cache_calc no hit_rate in output for %s (%.1fs): %s",
            fname, elapsed, result.stdout[:200],
        )
        return {"time": time_label, "hit_rate": None}
    except subprocess.TimeoutExpired:
        elapsed = _time.monotonic() - t0
        logger.error("[trend] cache_calc TIMEOUT for %s after %.1fs", fname, elapsed)
        return {"time": time_label, "hit_rate": None}
    except Exception as e:
        elapsed = _time.monotonic() - t0
        logger.error("[trend] cache_calc exception for %s (%.1fs): %s", fname, elapsed, e)
        return {"time": time_label, "hit_rate": None}


def _calc_stats(data_points: List[dict]) -> dict:
    """计算 mean / max / min"""
    rates = [d["hit_rate"] for d in data_points if d.get("hit_rate") is not None]
    if not rates:
        return {"mean": 0, "max": 0, "min": 0}
    return {
        "mean": round(sum(rates) / len(rates), 4),
        "max": round(max(rates), 4),
        "min": round(min(rates), 4),
    }


def _collect_model_outputs(
    task_data_dir: str,
    status_model_outputs: Optional[Dict] = None,
) -> Dict[str, List[str]]:
    """收集 per-model per-slice 文件，优先使用 status 中记录的路径，否则扫描目录"""
    model_outputs = {}
    if status_model_outputs:
        model_outputs = dict(status_model_outputs)

    if not model_outputs:
        tokenized_dir = os.path.join(task_data_dir, "tokenized")
        for txt_file in sorted(glob.glob(os.path.join(tokenized_dir, "**", "*_input_ids.txt"), recursive=True)):
            fname = os.path.basename(txt_file)
            m = re.match(r'kv_\d{8}_\d{6}_\d{8}_\d{6}_(.+)_input_ids\.txt$', fname)
            if m:
                model = m.group(1)
            else:
                model = fname.replace("_input_ids.txt", "").split("_")[-1]
            # glob 保证文件存在，仅需检查非空
            if os.path.getsize(txt_file) > 0:
                model_outputs.setdefault(model, []).append(txt_file)

    # 过滤空文件
    for model in list(model_outputs.keys()):
        model_outputs[model] = [
            f for f in model_outputs[model]
            if os.path.exists(f) and os.path.getsize(f) > 0
        ]
        if not model_outputs[model]:
            del model_outputs[model]

    return model_outputs


def compute_trend(
    task_data_dir: str,
    cache_calc_path: str = _DEFAULT_CACHE_CALC,
    cache_size: int = 200000000,
    block_size: int = 16,
    model_outputs: Optional[Dict[str, List[str]]] = None,
    max_workers: int = 8,
) -> dict:
    """
    计算分钟级命中率趋势数据。

    返回:
      {"series": [{"model": "...", "data": [...], "stats": {...}}, ...]}
    """
    t_start = _time.monotonic()

    if model_outputs is None:
        model_outputs = _collect_model_outputs(task_data_dir)

    if not model_outputs:
        logger.info("[trend] 无 model_outputs，跳过")
        return {"series": []}

    total_files = sum(len(fs) for fs in model_outputs.values())
    logger.info(
        "[trend] 开始计算: %d 模型, %d 文件, max_workers=%d",
        len(model_outputs), total_files, max_workers,
    )

    # ------------------------------------------------------------------
    # 去重：加载已有趋势结果，跳过已计算的 (model, time_label)
    # ------------------------------------------------------------------
    existing_results: Dict[tuple, float] = {}
    trend_file = os.path.join(task_data_dir, "report", "hit_rate_trend.json")
    if os.path.exists(trend_file):
        try:
            with open(trend_file, "r", encoding="utf-8") as f:
                prev = json.load(f)
            for ms in prev.get("series", []):
                m = ms.get("model")
                if m == "整体":
                    continue
                for d in ms.get("data", []):
                    if d.get("hit_rate") is not None:
                        existing_results[(m, d["time"])] = d["hit_rate"]
            if existing_results:
                logger.info("[trend] 加载已有结果 %d 条，将跳过重复计算", len(existing_results))
        except Exception as e:
            logger.warning("[trend] 读取已有 trend 文件失败，将全量计算: %s", e)
            existing_results = {}

    # ------------------------------------------------------------------
    # 构建任务列表：区分可复用缓存 vs 需计算的文件
    # ------------------------------------------------------------------
    work_items: List[tuple] = []       # (model, file_path)
    cached_items: List[tuple] = []     # (model, {"time": ..., "hit_rate": ...})

    for model, files in model_outputs.items():
        for f in files:
            if not os.path.exists(f) or os.path.getsize(f) == 0:
                continue
            fname = os.path.basename(f)
            time_label = _parse_time_from_filename(fname)
            cached_rate = existing_results.get((model, time_label))
            if cached_rate is not None:
                cached_items.append((model, {"time": time_label, "hit_rate": cached_rate}))
            else:
                work_items.append((model, f))

    logger.info(
        "[trend] 去重: %d 已有结果复用, %d 文件需计算",
        len(cached_items), len(work_items),
    )

    # ------------------------------------------------------------------
    # 单一共享 ThreadPoolExecutor 处理所有模型的文件
    # ------------------------------------------------------------------
    model_data: Dict[str, List[dict]] = {}

    # 先填入缓存结果
    for model, dp in cached_items:
        model_data.setdefault(model, []).append(dp)

    if work_items:
        done_count = 0
        workers = min(max_workers, len(work_items))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_model = {}
            for model, file_path in work_items:
                fut = pool.submit(_calc_single_file, file_path, cache_calc_path, cache_size, block_size)
                future_to_model[fut] = model

            for future in concurrent.futures.as_completed(future_to_model):
                model = future_to_model[future]
                dp = future.result()
                done_count += 1
                if dp and dp.get("hit_rate") is not None:
                    model_data.setdefault(model, []).append(dp)
                if done_count % 20 == 0 or done_count == len(work_items):
                    logger.info("[trend] 进度: %d/%d 文件完成", done_count, len(work_items))

    # ------------------------------------------------------------------
    # 构建 per-model series
    # ------------------------------------------------------------------
    model_series = []
    for model in sorted(model_data.keys()):
        data_points = sorted(model_data[model], key=lambda x: x["time"])
        if data_points:
            model_series.append({
                "model": model,
                "data": data_points,
                "stats": _calc_stats(data_points),
            })

    # ------------------------------------------------------------------
    # O(T*M) dict 聚合计算"整体"维度
    # ------------------------------------------------------------------
    series = list(model_series)

    if model_series:
        time_rates: Dict[str, List[float]] = {}
        for ms in model_series:
            for d in ms["data"]:
                if d.get("hit_rate") is not None:
                    time_rates.setdefault(d["time"], []).append(d["hit_rate"])

        overall_data = []
        for t in sorted(time_rates.keys()):
            rates = time_rates[t]
            overall_data.append({"time": t, "hit_rate": round(sum(rates) / len(rates), 4)})

        if overall_data:
            series.insert(0, {
                "model": "整体",
                "data": overall_data,
                "stats": _calc_stats(overall_data),
            })

    elapsed = _time.monotonic() - t_start
    total_points = sum(len(ms["data"]) for ms in model_series)
    logger.info(
        "[trend] 完成: %d 模型, %d 数据点, 耗时 %.1fs (复用 %d, 计算 %d)",
        len(model_series), total_points, elapsed,
        len(cached_items), len(work_items),
    )

    return {"series": series}


def compute_and_save(
    task_data_dir: str,
    cache_calc_path: str = _DEFAULT_CACHE_CALC,
    cache_size: int = 200000000,
    block_size: int = 16,
    model_outputs: Optional[Dict[str, List[str]]] = None,
    max_workers: int = 8,
) -> str:
    """
    计算趋势数据并保存到 {task_data_dir}/report/hit_rate_trend.json。
    返回输出文件路径。
    """
    logger.info("[trend] compute_and_save: %s", task_data_dir)
    result = compute_trend(
        task_data_dir, cache_calc_path, cache_size, block_size,
        model_outputs, max_workers,
    )
    report_dir = os.path.join(task_data_dir, "report")
    os.makedirs(report_dir, exist_ok=True)
    output_file = os.path.join(report_dir, "hit_rate_trend.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    n_series = len(result.get("series", []))
    logger.info("[trend] 已保存: %s (%d series)", output_file, n_series)
    return output_file


# ============================================================
# 命令行回填模式
# ============================================================
def backfill(status_dir: str, data_dir: str, force: bool = False):
    """扫描所有已完成任务，生成趋势数据"""
    cfg = _load_olap_config()
    cache_size = cfg.get("pipeline_cache_size", 200000000)
    block_size = cfg.get("pipeline_block_size", 16)

    task_count = 0
    skip_count = 0
    fail_count = 0

    for user_dir in sorted(os.listdir(status_dir)):
        user_status_dir = os.path.join(status_dir, user_dir)
        if not os.path.isdir(user_status_dir):
            continue
        for fname in sorted(os.listdir(user_status_dir)):
            if not fname.endswith(".json"):
                continue
            status_path = os.path.join(user_status_dir, fname)
            try:
                with open(status_path, "r", encoding="utf-8") as f:
                    status = json.load(f)
            except Exception:
                continue

            if status.get("is_deleted"):
                continue

            cur_stage = status.get("pipeline", {}).get("current_stage", "")
            if cur_stage not in ("done",):
                continue

            task_id = status.get("task_id", "")
            username = task_id.split("-kv_")[0] if "-kv_" in task_id else user_dir
            task_data_dir = os.path.join(data_dir, username, task_id)
            if not os.path.isdir(task_data_dir):
                continue

            output_file = os.path.join(task_data_dir, "report", "hit_rate_trend.json")
            if os.path.exists(output_file) and not force:
                skip_count += 1
                continue

            # 从 status 获取 model_outputs
            tokenize_stage = status.get("pipeline", {}).get("stages", {}).get("tokenize", {})
            status_model_outputs = tokenize_stage.get("model_outputs")
            model_outputs = _collect_model_outputs(task_data_dir, status_model_outputs)

            if not model_outputs:
                skip_count += 1
                continue

            print(f"[{task_count + 1}] 计算: {task_id} ({len(model_outputs)} 模型)...")
            try:
                path = compute_and_save(
                    task_data_dir, _DEFAULT_CACHE_CALC, cache_size, block_size, model_outputs
                )
                task_count += 1
                n_series = 0
                with open(path, "r") as f:
                    trend = json.load(f)
                    n_series = len(trend.get("series", []))
                print(f"    -> {path} ({n_series} series)")
            except Exception as e:
                fail_count += 1
                print(f"    [FAIL] {e}")

    print(f"\n{'=' * 50}")
    print(f"回填完成: 成功 {task_count}, 跳过 {skip_count}, 失败 {fail_count}")
    print(f"{'=' * 50}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="计算分钟级命中率趋势（回填已完成任务）")
    parser.add_argument("--status-dir", default=os.path.join(_BASE_DIR, "olap_database", "status"),
                        help="status 目录 (默认: olap_database/status)")
    parser.add_argument("--data-dir", default=os.path.join(_BASE_DIR, "olap_database", "data"),
                        help="data 目录 (默认: olap_database/data)")
    parser.add_argument("--force", action="store_true",
                        help="强制重新计算（覆盖已有的 hit_rate_trend.json）")
    args = parser.parse_args()

    print(f"status 目录: {args.status_dir}")
    print(f"data 目录:   {args.data_dir}")
    print(f"强制覆盖:   {args.force}")
    print()

    backfill(args.status_dir, args.data_dir, args.force)


if __name__ == "__main__":
    main()

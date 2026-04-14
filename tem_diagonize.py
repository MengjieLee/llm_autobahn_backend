#!/usr/bin/env python3
"""
Tokenize 性能诊断脚本 — 在 Pod 内执行
输出结果到 tem_diagonize.txt

用法:
    python3 tem_diagonize.py
"""
import time, sys, os, warnings

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from multiprocessing import Pool
from tokenize_script import (
    _worker_init, _worker_process_batch,
    TokenizerManager, MODEL_TOKENIZER_MAPPING,
)
import tokenize_script as _ts

OUTPUT = os.path.join(os.path.dirname(__file__), "tem_diagonize.txt")


def log(msg, fh=None):
    print(msg, flush=True)
    if fh:
        fh.write(msg + "\n")
        fh.flush()


def main():
    fh = open(OUTPUT, "w", encoding="utf-8")

    # ---- 1. 预加载 tokenizer ----
    log("=" * 70, fh)
    log("阶段 1: Tokenizer 加载测试", fh)
    log("=" * 70, fh)
    _ts._preloaded_tm = TokenizerManager()
    for m in MODEL_TOKENIZER_MAPPING:
        t0 = time.time()
        try:
            t, c = _ts._preloaded_tm.get_tokenizer(m)
            dt = time.time() - t0
            if t:
                ids = t.apply_chat_template([{"role": "user", "content": "hello"}])
                log(f"  {m:20s} -> {c:40s} | {dt:.1f}s | test_tokens={len(ids)} ✅", fh)
            else:
                log(f"  {m:20s} -> 无匹配 | {dt:.1f}s ❌", fh)
        except Exception as e:
            dt = time.time() - t0
            log(f"  {m:20s} -> 失败: {e} | {dt:.1f}s ❌", fh)

    # ---- 2. 查找测试数据 ----
    log("\n" + "=" * 70, fh)
    log("阶段 2: 读取测试数据", fh)
    log("=" * 70, fh)
    import glob
    candidates = sorted(
        glob.glob("olap_database/realtime/_task_data/*/filtered/_filtered_0.jsonl"),
        key=os.path.getmtime, reverse=True,
    )
    if not candidates:
        # 没有 filtered 文件，尝试原始 JSONL
        candidates = sorted(
            glob.glob("olap_database/realtime/_task_data/*/00/*.jsonl"),
            key=os.path.getmtime, reverse=True,
        )
    if not candidates:
        log("未找到测试文件，退出", fh)
        fh.close()
        return

    src = candidates[0]
    log(f"  文件: {src}", fh)
    log(f"  大小: {os.path.getsize(src) / 1024 / 1024:.0f} MB", fh)

    lines = []
    with open(src, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                lines.append((len(lines), line))
                if len(lines) >= 1000:
                    break
    log(f"  读取: {len(lines)} 条", fh)

    models = {"glm-5", "glm-5.1", "kimi-k2.5", "minimax-m2.5", "deepseek-v3.2"}

    # ---- 3. 多配置对比 ----
    log("\n" + "=" * 70, fh)
    log("阶段 3: Workers × BatchSize 对比 (1000 条)", fh)
    log("=" * 70, fh)

    configs = [
        (1,  200, "基线: 1w  b200"),
        (4,  200, "当前: 4w  b200"),
        (4,  50,  "小bat: 4w  b50"),
        (10, 200, "中等: 10w b200"),
        (10, 50,  "中等: 10w b50"),
        (20, 200, "大量: 20w b200"),
        (20, 50,  "大量: 20w b50"),
    ]

    header = f"  {'配置':<22s} | {'耗时':>6s} | {'吞吐':>10s} | {'成功':>5s} | {'失败':>5s} | {'加速比':>6s}"
    log(header, fh)
    log("  " + "-" * 75, fh)

    baseline_elapsed = None
    for num_workers, batch_size, label in configs:
        pool = Pool(
            processes=num_workers,
            initializer=_worker_init,
            initargs=(None, "glm-5", False),
        )
        max_pending = num_workers * 2
        batches = [lines[i:i + batch_size] for i in range(0, len(lines), batch_size)]

        t0 = time.time()
        pending = []
        success = 0
        failed = 0
        for batch in batches:
            future = pool.apply_async(_worker_process_batch, (batch, models))
            pending.append(future)
            if len(pending) >= max_pending:
                for fut in pending:
                    results, _ = fut.get()
                    for _, m, _ in results:
                        if m:
                            success += 1
                        else:
                            failed += 1
                pending.clear()
        for fut in pending:
            results, _ = fut.get()
            for _, m, _ in results:
                if m:
                    success += 1
                else:
                    failed += 1
        elapsed = time.time() - t0
        pool.terminate()
        pool.join()

        if baseline_elapsed is None:
            baseline_elapsed = elapsed
        speedup = baseline_elapsed / elapsed if elapsed > 0 else 0
        rps = len(lines) / elapsed
        log(f"  {label:<22s} | {elapsed:5.1f}s | {rps:7.1f} 条/s | {success:5d} | {failed:5d} | {speedup:5.2f}x", fh)

    # ---- 4. 失败原因分析 ----
    log("\n" + "=" * 70, fh)
    log("阶段 4: 失败原因分析 (前 100 条)", fh)
    log("=" * 70, fh)
    import json, re
    from tokenize_script import extract_request_body, extract_qianfan_model

    reasons = {"no_body": 0, "no_messages": 0, "no_tokenizer": 0, "too_long": 0, "ok": 0, "other": 0}
    model_dist = {}
    for idx, json_str in lines[:100]:
        try:
            record = json.loads(json_str)
            source = record.get("_source", {})
            raw_str = source.get("@raw", "")
            model_name = extract_qianfan_model(raw_str)
            model_dist[model_name] = model_dist.get(model_name, 0) + 1

            body = extract_request_body(raw_str)
            if not body:
                reasons["no_body"] += 1
                continue
            msgs = body.get("messages", [])
            if not msgs:
                reasons["no_messages"] += 1
                continue
            tok, _ = _ts._preloaded_tm.get_tokenizer(model_name or "glm-5")
            if tok is None:
                reasons["no_tokenizer"] += 1
                continue
            reasons["ok"] += 1
        except Exception:
            reasons["other"] += 1

    log("  失败原因分布:", fh)
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        log(f"    {reason:20s}: {count}", fh)

    log("\n  模型分布:", fh)
    for model, count in sorted(model_dist.items(), key=lambda x: -x[1]):
        log(f"    {model or '(unknown)':20s}: {count}", fh)

    log(f"\n结果已写入: {OUTPUT}", fh)
    fh.close()


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__) or ".")
    main()

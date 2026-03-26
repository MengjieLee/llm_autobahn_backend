#!/usr/bin/env python3
"""
运行缓存命中率模拟并生成结构化报告

用法:
    python cache_simulation.py \
        --input input_ids.txt \
        --output /path/to/s3_result/report.json \
        --cache-sizes 16,100,1000,0 \
        --block-size 200000000

示例:
    python cache_simulation.py \
        --input /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/src/domains/kv/cache_hit_rate/input_ids.txt \
        --output /mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend/olap_database/s3_result/cache_report.json \
        --cache-sizes 16,100,1000,0 \
        --block-size 200000000
"""

import argparse
import json
import os
import re
import subprocess
from datetime import datetime
from typing import List, Dict, Any


# 默认路径配置
BASE_DIR = "/mnt/cfs_bj_mt/workspace/limengjie03/tool_chain/llm_autobahn/llm_autobahn_backend"
DEFAULT_CACHE_CALC_PATH = f"{BASE_DIR}/src/domains/kv/cache_hit_rate/cache_calc"
DEFAULT_INPUT_DIR = f"{BASE_DIR}/olap_database/s2_result"
DEFAULT_OUTPUT_DIR = f"{BASE_DIR}/olap_database/s3_result"


def run_cache_calc(
    cache_calc_path: str,
    input_file: str,
    cache_sizes: List[int],
    block_size: int,
    use_timestamp: bool = False,
    use_prefix_hash: bool = True
) -> Dict[str, Any]:
    """
    运行 cache_calc 并解析输出
    """
    # 构建命令
    cmd = [cache_calc_path, "-f", input_file]

    for size in cache_sizes:
        cmd.extend(["-s", str(size)])

    cmd.extend(["-b", str(block_size)])

    if use_timestamp:
        cmd.extend(["-t", "true"])

    if not use_prefix_hash:
        cmd.extend(["-p", "false"])

    print(f"[INFO] 执行命令: {' '.join(cmd)}")

    # 执行命令
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"cache_calc 执行失败: {result.stderr}")

    output = result.stdout.strip()
    print(f"[INFO] 原始输出:\n{output}")

    return parse_cache_calc_output(output)


def parse_cache_calc_output(output: str) -> Dict[str, Any]:
    """
    解析 cache_calc 输出

    输入格式:
        entries: 3, tokens: 4188
        cache_size: 16  total_adds: 3   hit_count: 0    hit_rate: 0
    """
    lines = output.strip().split('\n')

    result = {
        "summary": {},
        "cache_results": []
    }

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 解析 entries 和 tokens
        if line.startswith("entries:"):
            match = re.match(r'entries:\s*(\d+),\s*tokens:\s*(\d+)', line)
            if match:
                result["summary"]["total_entries"] = int(match.group(1))
                result["summary"]["total_tokens"] = int(match.group(2))

        # 解析 cache_size 行
        elif line.startswith("cache_size:"):
            # cache_size: 16  total_adds: 3   hit_count: 0    hit_rate: 0
            parts = re.findall(r'(\w+):\s*([\d.]+)', line)
            if parts:
                cache_result = {}
                for key, value in parts:
                    if key == "hit_rate":
                        cache_result[key] = float(value)
                    else:
                        cache_result[key] = int(value)

                # 计算 miss 相关指标
                if "total_adds" in cache_result and "hit_count" in cache_result:
                    cache_result["miss_count"] = cache_result["total_adds"] - cache_result["hit_count"]
                    # hit_rate 已是 0~1 比率 (来自 cache_calc: hit_count / total_adds)
                    cache_result["miss_rate"] = 1 - cache_result.get("hit_rate", 0)

                result["cache_results"].append(cache_result)

    return result


def generate_report(
    input_file: str,
    cache_results: Dict[str, Any],
    cache_sizes: List[int],
    block_size: int,
    use_prefix_hash: bool
) -> Dict[str, Any]:
    """
    生成结构化报告
    """
    now = datetime.now()

    report = {
        "meta": {
            "report_id": now.strftime("%Y%m%d_%H%M%S"),
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "input_file": input_file,
            "input_file_name": os.path.basename(input_file),
        },
        "config": {
            "cache_sizes": cache_sizes,
            "block_size": block_size,
            "block_size_readable": format_number(block_size),
            "use_prefix_hash": use_prefix_hash,
            "algorithm": "LRU",
        },
        "summary": {
            "total_entries": cache_results["summary"].get("total_entries", 0),
            "total_tokens": cache_results["summary"].get("total_tokens", 0),
            "avg_tokens_per_entry": 0,
        },
        "results": [],
        "analysis": {}
    }

    # 计算平均 token 数
    if report["summary"]["total_entries"] > 0:
        report["summary"]["avg_tokens_per_entry"] = round(
            report["summary"]["total_tokens"] / report["summary"]["total_entries"], 2
        )

    # 处理每个 cache_size 的结果
    for cr in cache_results["cache_results"]:
        cache_size = cr.get("cache_size", 0)
        result_item = {
            "cache_size": cache_size,
            "cache_size_readable": "无限" if cache_size == 0 else format_number(cache_size),
            "total_queries": cr.get("total_adds", 0),
            "hit_count": cr.get("hit_count", 0),
            "miss_count": cr.get("miss_count", 0),
            "hit_rate": cr.get("hit_rate", 0),
            "hit_rate_percent": round(cr.get("hit_rate", 0) * 100, 2),
            "miss_rate": cr.get("miss_rate", 1),
            "miss_rate_percent": round(cr.get("miss_rate", 1) * 100, 2),
        }
        report["results"].append(result_item)

    # 生成分析
    if report["results"]:
        best = max(report["results"], key=lambda x: x["hit_rate"])
        worst = min(report["results"], key=lambda x: x["hit_rate"])

        report["analysis"] = {
            "best_config": {
                "cache_size": best["cache_size"],
                "hit_rate_percent": best["hit_rate_percent"],
            },
            "worst_config": {
                "cache_size": worst["cache_size"],
                "hit_rate_percent": worst["hit_rate_percent"],
            },
            "recommendation": generate_recommendation(report["results"]),
        }

    return report


def format_number(n: int) -> str:
    """格式化大数字为可读形式"""
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    elif n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def generate_recommendation(results: List[Dict]) -> str:
    """根据结果生成建议"""
    if not results:
        return "数据不足，无法生成建议"

    # 找到命中率最高的配置
    best = max(results, key=lambda x: x["hit_rate"])

    if best["hit_rate"] == 0:
        return "所有配置命中率均为0，建议检查数据是否有重复前缀，或增加测试数据量"
    elif best["hit_rate"] < 0.1:
        return f"命中率较低 ({best['hit_rate_percent']}%)，请求间重复度不高，缓存收益有限"
    elif best["hit_rate"] < 0.3:
        return f"命中率一般 ({best['hit_rate_percent']}%)，建议缓存大小设置为 {best['cache_size']}"
    elif best["hit_rate"] < 0.5:
        return f"命中率中等 ({best['hit_rate_percent']}%)，缓存效果明显，推荐配置: cache_size={best['cache_size']}"
    else:
        return f"命中率较高 ({best['hit_rate_percent']}%)，缓存效果显著，推荐配置: cache_size={best['cache_size']}"


def main():
    parser = argparse.ArgumentParser(description="运行缓存命中率模拟并生成报告")
    parser.add_argument("--input", "-i", required=True, help="input_ids.txt 文件路径")
    parser.add_argument("--output", "-o", required=True, help="输出报告路径 (JSON)")
    parser.add_argument("--cache-sizes", "-s", default="16",
                        help="缓存大小列表，逗号分隔 (默认: 16)")
    parser.add_argument("--block-size", "-b", type=int, default=200000000,
                        help="block 大小 (token 数，默认: 200000000)")
    parser.add_argument("--cache-calc", default=DEFAULT_CACHE_CALC_PATH,
                        help="cache_calc 可执行文件路径")
    parser.add_argument("--no-prefix-hash", action="store_true",
                        help="禁用前缀哈希")
    parser.add_argument("--with-timestamp", "-t", action="store_true",
                        help="启用时间戳处理")

    args = parser.parse_args()

    # 解析 cache_sizes
    cache_sizes = [int(s.strip()) for s in args.cache_sizes.split(",")]

    # 检查输入文件
    if not os.path.exists(args.input):
        print(f"[ERROR] 输入文件不存在: {args.input}")
        return 1

    # 检查 cache_calc
    if not os.path.exists(args.cache_calc):
        print(f"[ERROR] cache_calc 不存在: {args.cache_calc}")
        return 1

    print(f"[INFO] 输入文件: {args.input}")
    print(f"[INFO] 输出文件: {args.output}")
    print(f"[INFO] 缓存大小: {cache_sizes}")
    print(f"[INFO] Block 大小: {args.block_size}")

    # 运行 cache_calc
    try:
        cache_results = run_cache_calc(
            cache_calc_path=args.cache_calc,
            input_file=args.input,
            cache_sizes=cache_sizes,
            block_size=args.block_size,
            use_timestamp=args.with_timestamp,
            use_prefix_hash=not args.no_prefix_hash
        )
    except Exception as e:
        print(f"[ERROR] 执行失败: {e}")
        return 1

    # 生成报告
    report = generate_report(
        input_file=args.input,
        cache_results=cache_results,
        cache_sizes=cache_sizes,
        block_size=args.block_size,
        use_prefix_hash=not args.no_prefix_hash
    )

    # 创建输出目录
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 保存报告
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n[INFO] 报告已保存: {args.output}")

    # 打印摘要
    print("\n" + "=" * 60)
    print("缓存命中率模拟报告")
    print("=" * 60)
    print(f"总请求数: {report['summary']['total_entries']}")
    print(f"总 Token 数: {report['summary']['total_tokens']}")
    print(f"平均 Token/请求: {report['summary']['avg_tokens_per_entry']}")
    print("-" * 60)
    print(f"{'Cache Size':<15} {'命中数':<12} {'命中率':<12}")
    print("-" * 60)
    for r in report["results"]:
        print(f"{r['cache_size_readable']:<15} {r['hit_count']:<12} {r['hit_rate_percent']}%")
    print("-" * 60)
    print(f"建议: {report['analysis'].get('recommendation', 'N/A')}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    exit(main())

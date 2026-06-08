#!/usr/bin/env python3
"""KV-Cache 理论命中率归因分析脚本。"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.domains.kv.attribution_service import (  # noqa: E402
    DEFAULT_SCENARIO,
    apply_llm_summary,
    build_attribution_report,
    render_markdown_report,
)

REPORTS_DIR = BASE_DIR / "olap_database" / "daily_reports"
OUTPUT_DIR = BASE_DIR / "olap_database" / "attribution_reports"

logger = logging.getLogger(__name__)


def run(
    start_date: str,
    end_date: str,
    scenario: str = DEFAULT_SCENARIO,
    use_llm: bool = True,
    k: float = 1.5,
    min_delta_pp: float = 2.0,
    max_days: int = 5,
    weight_metric: str = "total_queries",
    top_n: int = 5,
) -> dict:
    """生成归因 JSON 与 Markdown 报告。"""
    logger.info("归因分析范围: %s ~ %s, 场景: %s", start_date, end_date, scenario)
    report = build_attribution_report(
        start_date,
        end_date,
        scenario=scenario,
        reports_dir=REPORTS_DIR,
        k=k,
        min_delta_pp=min_delta_pp,
        max_days=max_days,
        weight_metric=weight_metric,
        top_n=top_n,
    )

    if use_llm:
        report = apply_llm_summary(report)

    markdown = render_markdown_report(report)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_scenario = "".join(ch if ch.isalnum() or ch in "._-~" else "_" for ch in scenario)
    prefix = f"{start_date}_{end_date}_{safe_scenario}"
    json_path = OUTPUT_DIR / f"{prefix}.json"
    markdown_path = OUTPUT_DIR / f"{prefix}.md"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    with open(markdown_path, "w", encoding="utf-8") as f:
        f.write(markdown)

    logger.info("归因 JSON 已保存: %s", json_path)
    logger.info("归因 Markdown 已保存: %s", markdown_path)
    return {**report, "output_files": {"json": str(json_path), "markdown": str(markdown_path)}}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KV-Cache 理论命中率归因分析")
    parser.add_argument("--start", help="开始日期 MM-DD")
    parser.add_argument("--end", help="结束日期 MM-DD")
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO)
    parser.add_argument("--k", type=float, default=1.5, help="IQR 异常检测灵敏度，越小越敏感")
    parser.add_argument("--min-delta-pp", type=float, default=2.0, help="纳入关注的最小变化百分点")
    parser.add_argument("--max-days", type=int, default=5, help="最多输出关注峰谷日数量")
    parser.add_argument("--weight-metric", choices=["total_queries", "total_tokens"], default="total_queries")
    parser.add_argument("--top-n", type=int, default=5, help="每个关注日输出 Top N 正/负贡献项")
    parser.add_argument("--llm", action="store_true", help="启用 LLM 总结，默认已启用，需设置 ATTRIBUTION_LLM_API_KEY")
    parser.add_argument("--no-llm", action="store_true", help="禁用 LLM 总结")
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=29)
    result = run(
        args.start or start.strftime("%m-%d"),
        args.end or end.strftime("%m-%d"),
        scenario=args.scenario,
        use_llm=not args.no_llm,
        k=args.k,
        min_delta_pp=args.min_delta_pp,
        max_days=args.max_days,
        weight_metric=args.weight_metric,
        top_n=args.top_n,
    )
    print(render_markdown_report(result))


if __name__ == "__main__":
    main()

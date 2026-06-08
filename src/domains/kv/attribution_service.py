"""KV-Cache 理论命中率变化归因服务。"""

from __future__ import annotations

import json
import logging
import os
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "olap_database" / "daily_reports"
DEFAULT_SCENARIO = "20~21_全场景_各模型"
VALID_WEIGHT_METRICS = {"total_queries", "total_tokens"}

logger = logging.getLogger(__name__)


def _load_dotenv_file():
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return

    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key or key in os.environ:
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            os.environ[key] = value
    except OSError as exc:
        logger.warning("读取 .env 失败: %s", exc)


def llm_summarize(report: dict) -> str | None:
    """调用 LLM 生成摘要；凭据必须来自环境变量或项目 .env。"""
    _load_dotenv_file()
    api_key = os.environ.get("ATTRIBUTION_LLM_API_KEY")
    if not api_key:
        logger.warning("未设置 ATTRIBUTION_LLM_API_KEY，跳过 LLM 总结")
        return None

    import httpx

    base_url = os.environ.get("ATTRIBUTION_LLM_BASE_URL", "https://oneapi-comate.baidu-int.com/v1")
    model = os.environ.get("ATTRIBUTION_LLM_MODEL", "gpt-5.5")
    prompt = f"""你是 KV-Cache 命中率分析专家。以下是一段时间窗口内的命中率归因分析数据，请用简洁中文总结：
1. 整体趋势概述（一句话）
2. 异常天的核心原因（每天2-3句，聚焦最大贡献模型）
3. 可能的业务解释和建议

数据：
{json.dumps(report, ensure_ascii=False, indent=2)}

要求：面向技术管理者汇报风格，简洁、有数据支撑，不超过300字。"""

    response = httpx.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 800,
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


def apply_llm_summary(report: dict) -> dict:
    try:
        llm_summary = llm_summarize(report)
        if llm_summary:
            report["summary"] = llm_summary
            report["llm_summary"] = llm_summary
    except Exception as exc:
        logger.warning("LLM 总结失败: %s", exc)
    return report


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _date_range_labels(start_date: str, end_date: str) -> Iterable[str]:
    try:
        start = datetime.strptime(start_date, "%m-%d")
        end = datetime.strptime(end_date, "%m-%d")
    except ValueError as exc:
        raise ValueError("日期格式必须为 MM-DD") from exc

    if end < start:
        end = end.replace(year=start.year + 1)

    current = start
    while current <= end:
        yield current.strftime("%m-%d")
        current += timedelta(days=1)


def _rate_from_counts(hit_count: int, total_queries: int) -> float:
    return hit_count / total_queries * 100 if total_queries > 0 else 0.0


def _extract_model_metrics(model_stats: dict) -> dict:
    hit_count = _safe_int(model_stats.get("hit_count"))
    total_queries = _safe_int(model_stats.get("total_queries"))
    total_tokens = _safe_int(model_stats.get("total_tokens"))
    hit_rate = _safe_float(model_stats.get("hit_rate_percent"))
    if hit_rate == 0 and hit_count > 0 and total_queries > 0:
        hit_rate = _rate_from_counts(hit_count, total_queries)

    return {
        "hit_rate": hit_rate,
        "hit_count": hit_count,
        "total_queries": total_queries,
        "total_tokens": total_tokens,
    }


def _extract_daily_record(date_label: str, scenario_data: dict) -> dict | None:
    models_data = scenario_data.get("models")
    if not isinstance(models_data, dict) or not models_data:
        return None

    models = {}
    for model, stats in models_data.items():
        if isinstance(stats, dict):
            models[model] = _extract_model_metrics(stats)

    if not models:
        return None

    hit_count = _safe_int(scenario_data.get("hit_count"))
    total_queries = _safe_int(scenario_data.get("total_queries"))
    total_tokens = _safe_int(scenario_data.get("total_tokens"))

    if hit_count == 0:
        hit_count = sum(m["hit_count"] for m in models.values())
    if total_queries == 0:
        total_queries = sum(m["total_queries"] for m in models.values())
    if total_tokens == 0:
        total_tokens = sum(m["total_tokens"] for m in models.values())

    hit_rate = _safe_float(scenario_data.get("hit_rate_percent"))
    if hit_rate == 0 and hit_count > 0 and total_queries > 0:
        hit_rate = _rate_from_counts(hit_count, total_queries)

    return {
        "date": date_label,
        "hit_rate": hit_rate,
        "hit_count": hit_count,
        "total_queries": total_queries,
        "total_tokens": total_tokens,
        "models": models,
        "updated_at": scenario_data.get("updated_at", ""),
    }


def load_daily_records(
    start_date: str,
    end_date: str,
    scenario: str = DEFAULT_SCENARIO,
    reports_dir: str | Path = DEFAULT_REPORTS_DIR,
) -> list[dict]:
    """读取指定日期窗口和场景的日报记录。"""
    base_dir = Path(reports_dir)
    records = []

    for date_label in _date_range_labels(start_date, end_date):
        report_file = base_dir / f"{date_label}.json"
        if not report_file.exists():
            continue

        try:
            with open(report_file, "r", encoding="utf-8") as f:
                report = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue

        scenario_data = report.get("scenarios", {}).get(scenario)
        if not isinstance(scenario_data, dict):
            continue

        record = _extract_daily_record(date_label, scenario_data)
        if record:
            records.append(record)

    return records


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def select_focus_days(
    records: list[dict],
    k: float = 1.5,
    min_delta_pp: float = 2.0,
    max_days: int = 5,
) -> list[dict]:
    """按 IQR + 最小变化阈值选择峰谷关注日。"""
    if max_days <= 0 or len(records) < 2:
        return []
    if k < 0:
        raise ValueError("k 不能小于 0")

    rates = [r["hit_rate"] for r in records]
    median_rate = _median(rates)
    lower = None
    upper = None

    if len(rates) >= 4:
        q1, _, q3 = statistics.quantiles(rates, n=4, method="inclusive")
        iqr = q3 - q1
        lower = q1 - k * iqr
        upper = q3 + k * iqr

    focus_days = []
    for record in records:
        rate = record["hit_rate"]
        delta_pp = rate - median_rate
        is_valley = (lower is not None and rate < lower) or delta_pp <= -min_delta_pp
        is_peak = (upper is not None and rate > upper) or delta_pp >= min_delta_pp
        if not is_valley and not is_peak:
            continue

        focus_days.append({
            "date": record["date"],
            "type": "valley" if is_valley else "peak",
            "hit_rate": round(rate, 2),
            "baseline_hit_rate": round(median_rate, 2),
            "delta_pp": round(delta_pp, 2),
            "threshold": {
                "lower": round(lower, 2) if lower is not None else None,
                "upper": round(upper, 2) if upper is not None else None,
                "min_delta_pp": min_delta_pp,
            },
        })

    focus_days.sort(key=lambda item: abs(item["delta_pp"]), reverse=True)
    return focus_days[:max_days]


def _snapshot(records: list[dict], weight_metric: str) -> dict:
    model_totals: dict[str, dict] = {}
    for record in records:
        for model, metrics in record["models"].items():
            bucket = model_totals.setdefault(model, {
                "hit_count": 0,
                "total_queries": 0,
                "total_tokens": 0,
            })
            bucket["hit_count"] += metrics["hit_count"]
            bucket["total_queries"] += metrics["total_queries"]
            bucket["total_tokens"] += metrics["total_tokens"]

    total_weight = sum(m[weight_metric] for m in model_totals.values())
    models = {}
    for model, metrics in model_totals.items():
        rate = _rate_from_counts(metrics["hit_count"], metrics["total_queries"])
        weight = metrics[weight_metric]
        share = weight / total_weight if total_weight > 0 else 0.0
        models[model] = {
            "hit_rate": rate,
            "share": share,
            "hit_count": metrics["hit_count"],
            "total_queries": metrics["total_queries"],
            "total_tokens": metrics["total_tokens"],
        }

    weighted_hit_rate = sum(m["share"] * m["hit_rate"] for m in models.values())
    return {
        "hit_rate": weighted_hit_rate,
        "total_weight": total_weight,
        "models": models,
    }


def _classify_reason(item: dict, baseline_overall: float) -> tuple[str, str]:
    structural = item["structural_pp"]
    performance = item["performance_pp"]
    contribution = item["contribution_pp"]
    current_share = item["current_share"]
    baseline_share = item["baseline_share"]
    current_rate = item["current_hit_rate"]
    baseline_rate = item["baseline_hit_rate"]

    structural_dominant = abs(structural) >= abs(performance) * 1.2
    performance_dominant = abs(performance) >= abs(structural) * 1.2

    if structural_dominant:
        if contribution < 0:
            if current_share > baseline_share and baseline_rate < baseline_overall:
                return "low_hit_traffic_share_increase", "低命中模型流量占比上升"
            if current_share < baseline_share and baseline_rate > baseline_overall:
                return "high_hit_traffic_share_decrease", "高命中模型流量占比下降"
            return "negative_traffic_mix_shift", "流量结构变化带来负向影响"
        if current_share > baseline_share and baseline_rate > baseline_overall:
            return "high_hit_traffic_share_increase", "高命中模型流量占比上升"
        if current_share < baseline_share and baseline_rate < baseline_overall:
            return "low_hit_traffic_share_decrease", "低命中模型流量占比下降"
        return "positive_traffic_mix_shift", "流量结构变化带来正向影响"

    if performance_dominant:
        if contribution < 0 and current_rate < baseline_rate:
            return "model_hit_rate_drop", "模型自身命中率下降"
        if contribution > 0 and current_rate > baseline_rate:
            return "model_hit_rate_improve", "模型自身命中率提升"
        return "model_hit_rate_change", "模型自身命中率变化"

    return "mixed", "流量结构与模型自身命中率共同变化"


def compute_day_attribution(
    current_record: dict,
    baseline_records: list[dict],
    weight_metric: str = "total_queries",
    top_n: int | None = None,
) -> dict:
    """计算单日相对基准窗口的模型级归因。"""
    if weight_metric not in VALID_WEIGHT_METRICS:
        raise ValueError(f"weight_metric 仅支持: {', '.join(sorted(VALID_WEIGHT_METRICS))}")
    if not baseline_records:
        raise ValueError("baseline_records 不能为空")

    current = _snapshot([current_record], weight_metric)
    baseline = _snapshot(baseline_records, weight_metric)
    baseline_overall = baseline["hit_rate"]
    all_models = sorted(set(current["models"]) | set(baseline["models"]))

    attribution = []
    for model in all_models:
        cur = current["models"].get(model, {})
        base = baseline["models"].get(model, {})
        current_share = cur.get("share", 0.0)
        baseline_share = base.get("share", 0.0)
        current_rate = cur.get("hit_rate", base.get("hit_rate", 0.0))
        baseline_rate = base.get("hit_rate", cur.get("hit_rate", 0.0))

        share_delta = current_share - baseline_share
        rate_delta = current_rate - baseline_rate
        structural = share_delta * (baseline_rate - baseline_overall)
        performance = baseline_share * rate_delta
        interaction = share_delta * rate_delta
        structural_adj = structural + 0.5 * interaction
        performance_adj = performance + 0.5 * interaction
        contribution = structural_adj + performance_adj

        item = {
            "model": model,
            "contribution_pp": round(contribution, 4),
            "structural_pp": round(structural_adj, 4),
            "performance_pp": round(performance_adj, 4),
            "current_hit_rate": round(current_rate, 2),
            "baseline_hit_rate": round(baseline_rate, 2),
            "hit_rate_delta_pp": round(rate_delta, 2),
            "current_share": round(current_share * 100, 2),
            "baseline_share": round(baseline_share * 100, 2),
            "share_delta_pp": round(share_delta * 100, 2),
            "current_total_queries": cur.get("total_queries", 0),
            "baseline_total_queries": base.get("total_queries", 0),
            "current_total_tokens": cur.get("total_tokens", 0),
            "baseline_total_tokens": base.get("total_tokens", 0),
        }
        reason_type, reason = _classify_reason(item, baseline_overall)
        item["reason_type"] = reason_type
        item["reason"] = reason
        attribution.append(item)

    attribution.sort(key=lambda item: item["contribution_pp"])
    if top_n is not None:
        negative = [item for item in attribution if item["contribution_pp"] < 0][:top_n]
        positive = [item for item in reversed(attribution) if item["contribution_pp"] > 0][:top_n]
        selected_models = {item["model"] for item in negative + positive}
        attribution = [item for item in attribution if item["model"] in selected_models]
        attribution.sort(key=lambda item: item["contribution_pp"])

    delta_pp = current["hit_rate"] - baseline["hit_rate"]
    return {
        "date": current_record["date"],
        "current_hit_rate": round(current["hit_rate"], 2),
        "baseline_hit_rate": round(baseline["hit_rate"], 2),
        "delta_pp": round(delta_pp, 4),
        "baseline_days": len(baseline_records),
        "attribution": attribution,
        "top_negative": [item for item in attribution if item["contribution_pp"] < 0][:top_n or 5],
        "top_positive": [item for item in reversed(attribution) if item["contribution_pp"] > 0][:top_n or 5],
    }


def _build_summary(report: dict) -> str:
    if report["total_days"] == 0:
        return "指定窗口内未找到可用于归因的日报数据。"
    if not report["focus_days"]:
        return (
            f"{report['range']} 场景 {report['scenario']} 共加载 {report['total_days']} 天数据，"
            f"未发现超过阈值的显著峰谷变化。"
        )

    day = report["focus_days"][0]
    direction = "下降" if day["delta_pp"] < 0 else "上升"
    drivers = day["top_negative"] if day["delta_pp"] < 0 else day["top_positive"]
    if not drivers:
        return (
            f"{day['date']} 命中率 {day['hit_rate']:.2f}%，较基准 {day['baseline_hit_rate']:.2f}% "
            f"{direction} {abs(day['delta_pp']):.2f}pp，但未找到显著模型级贡献项。"
        )

    top = drivers[0]
    return (
        f"{day['date']} 命中率 {day['hit_rate']:.2f}%，较基准 {day['baseline_hit_rate']:.2f}% "
        f"{direction} {abs(day['delta_pp']):.2f}pp。主要贡献模型为 {top['model']}，"
        f"贡献 {top['contribution_pp']:.2f}pp，其中结构贡献 {top['structural_pp']:.2f}pp、"
        f"模型自身命中率贡献 {top['performance_pp']:.2f}pp，判断为{top['reason']}。"
    )


def build_attribution_report(
    start_date: str,
    end_date: str,
    scenario: str = DEFAULT_SCENARIO,
    reports_dir: str | Path = DEFAULT_REPORTS_DIR,
    k: float = 1.5,
    min_delta_pp: float = 2.0,
    max_days: int = 5,
    weight_metric: str = "total_queries",
    top_n: int = 5,
) -> dict:
    """生成完整归因报告。"""
    if weight_metric not in VALID_WEIGHT_METRICS:
        raise ValueError(f"weight_metric 仅支持: {', '.join(sorted(VALID_WEIGHT_METRICS))}")

    records = load_daily_records(start_date, end_date, scenario, reports_dir)
    trend = [
        {
            "date": record["date"],
            "hit_rate": round(record["hit_rate"], 2),
            "hit_count": record["hit_count"],
            "total_queries": record["total_queries"],
            "total_tokens": record["total_tokens"],
        }
        for record in records
    ]

    focus = select_focus_days(records, k=k, min_delta_pp=min_delta_pp, max_days=max_days)
    records_by_date = {record["date"]: record for record in records}
    focus_days = []

    for focus_day in focus:
        current_record = records_by_date[focus_day["date"]]
        baseline_records = [record for record in records if record["date"] != focus_day["date"]]
        if not baseline_records:
            continue
        attribution_result = compute_day_attribution(
            current_record,
            baseline_records,
            weight_metric=weight_metric,
            top_n=top_n,
        )
        focus_days.append({
            **focus_day,
            "baseline_hit_rate": attribution_result["baseline_hit_rate"],
            "delta_pp": round(attribution_result["delta_pp"], 2),
            "baseline_days": attribution_result["baseline_days"],
            "attribution": attribution_result["attribution"],
            "top_negative": attribution_result["top_negative"],
            "top_positive": attribution_result["top_positive"],
        })

    rates = [record["hit_rate"] for record in records]
    report = {
        "range": f"{start_date} ~ {end_date}",
        "scenario": scenario,
        "weight_metric": weight_metric,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_days": len(records),
        "baseline_hit_rate": round(_median(rates), 2),
        "mean_hit_rate": round(sum(rates) / len(rates), 2) if rates else 0,
        "min_hit_rate": round(min(rates), 2) if rates else 0,
        "max_hit_rate": round(max(rates), 2) if rates else 0,
        "trend": trend,
        "focus_days": focus_days,
    }
    report["summary"] = _build_summary(report)
    return report


def render_markdown_report(report: dict) -> str:
    """将归因 JSON 渲染为 Markdown 汇报。"""
    lines = [
        "# KV-Cache 理论命中率变化归因报告",
        "",
        "## 总览",
        f"- 时间窗口：{report['range']}",
        f"- 场景：{report['scenario']}",
        f"- 权重口径：{report['weight_metric']}",
        f"- 数据天数：{report['total_days']}",
        f"- 中位命中率：{report['baseline_hit_rate']:.2f}%",
        f"- 平均命中率：{report['mean_hit_rate']:.2f}%",
        f"- 峰谷范围：{report['min_hit_rate']:.2f}% ~ {report['max_hit_rate']:.2f}%",
        "",
        "## 结论摘要",
        report.get("summary") or "无",
        "",
    ]

    if report.get("llm_summary") and report.get("llm_summary") != report.get("summary"):
        lines.extend([
            "## LLM 总结",
            report["llm_summary"],
            "",
        ])

    if not report["focus_days"]:
        lines.extend(["## 关注峰谷日", "未发现超过阈值的显著峰谷变化。", ""])
        return "\n".join(lines)

    lines.extend([
        "## 关注峰谷日",
        "| 日期 | 类型 | 当前命中率 | 基准命中率 | 变化 | 基准天数 |",
        "|---|---|---:|---:|---:|---:|",
    ])
    for day in report["focus_days"]:
        day_type = "低谷" if day["type"] == "valley" else "峰值"
        lines.append(
            f"| {day['date']} | {day_type} | {day['hit_rate']:.2f}% | "
            f"{day['baseline_hit_rate']:.2f}% | {day['delta_pp']:.2f}pp | {day['baseline_days']} |"
        )
    lines.append("")

    for day in report["focus_days"]:
        day_type = "低谷" if day["type"] == "valley" else "峰值"
        lines.extend([
            f"## {day['date']} {day_type}归因",
            "",
            "### Top 负向贡献",
            "| 模型 | 总贡献 | 结构贡献 | 命中率贡献 | 当前占比 | 基准占比 | 当前命中率 | 基准命中率 | 判断 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ])
        negative = day.get("top_negative") or []
        if negative:
            for item in negative:
                lines.append(
                    f"| {item['model']} | {item['contribution_pp']:.2f}pp | {item['structural_pp']:.2f}pp | "
                    f"{item['performance_pp']:.2f}pp | {item['current_share']:.2f}% | "
                    f"{item['baseline_share']:.2f}% | {item['current_hit_rate']:.2f}% | "
                    f"{item['baseline_hit_rate']:.2f}% | {item['reason']} |"
                )
        else:
            lines.append("| - | - | - | - | - | - | - | - | - |")

        lines.extend([
            "",
            "### Top 正向抵消",
            "| 模型 | 总贡献 | 结构贡献 | 命中率贡献 | 当前占比 | 基准占比 | 当前命中率 | 基准命中率 | 判断 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ])
        positive = day.get("top_positive") or []
        if positive:
            for item in positive:
                lines.append(
                    f"| {item['model']} | {item['contribution_pp']:.2f}pp | {item['structural_pp']:.2f}pp | "
                    f"{item['performance_pp']:.2f}pp | {item['current_share']:.2f}% | "
                    f"{item['baseline_share']:.2f}% | {item['current_hit_rate']:.2f}% | "
                    f"{item['baseline_hit_rate']:.2f}% | {item['reason']} |"
                )
        else:
            lines.append("| - | - | - | - | - | - | - | - | - |")
        lines.append("")

    return "\n".join(lines)

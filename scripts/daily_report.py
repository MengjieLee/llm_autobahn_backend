#!/usr/bin/env python3
"""
每日 KV Cache 命中率报告推送脚本

功能：
- 查找前一天的已完成日报任务（全场景、coding_plan、各应用等）
- 汇总各场景各模型命中率数据
- 写入 olap_database/daily_reports/{MM-DD}.json
- 推送全场景日报到 IM 机器人群

用法：
- 手动执行: python scripts/daily_report.py
- crontab:  0 10 * * * cd /path/to/backend && python scripts/daily_report.py >> logs/daily/daily_report_$(date +%%Y-%%m-%%d).log 2>&1

配置读取 olap_config.json 中的:
- notify_im_bot_url
- notify_im_bot_toid
"""

import json
import glob
import re
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent
STATUS_DIR = BASE_DIR / "olap_database" / "status"
DAILY_REPORTS_DIR = BASE_DIR / "olap_database" / "daily_reports"
CONFIG_FILE = BASE_DIR / "app" / "conf" / "olap_config.json"


def load_config():
    """加载 olap 配置"""
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


from typing import Optional, List


def extract_scenario_name(task_name: str, target_date: str) -> Optional[str]:
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

    # 去掉日期前缀
    target_mm, target_dd = target_date.split("-")
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


def find_daily_tasks(target_date: str) -> List[dict]:
    """
    查找指定日期的所有已完成日报任务。
    target_date: "04-01" 格式
    返回按 updated_at 降序排列的任务列表
    """
    tasks = []

    for status_file in STATUS_DIR.glob(f"**/*.json"):
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            task_name = data.get("task_name", "")
            if target_date not in task_name:
                continue
            stage = data.get("pipeline", {}).get("current_stage", "")
            if stage != "done" or not data.get("result") or not isinstance(data["result"], dict):
                continue

            # 只保留来自 daily_tasks.json 的任务（以 MM-DD_ 开头或带 【】前缀）
            scenario = extract_scenario_name(task_name, target_date)
            if not scenario:
                continue

            tasks.append(data)
        except Exception:
            continue

    # 按 updated_at 降序
    tasks.sort(key=lambda t: t.get("updated_at", ""), reverse=True)
    return tasks


def save_daily_report(date_label: str, tasks: List[dict]) -> int:
    """
    将指定日期的所有已完成任务写入 daily_reports/{MM-DD}.json。
    每个场景只保留最新的一条任务数据。
    返回写入的场景数量。
    """
    DAILY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    filepath = DAILY_REPORTS_DIR / f"{date_label}.json"

    # 读取已有数据
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
        if not result or not isinstance(result, dict):
            continue

        scenario = extract_scenario_name(task_name, date_label)
        if not scenario:
            continue

        # 如果该场景已存在，跳过（保留已有的最新数据）
        if scenario in scenarios:
            continue

        # 计算整体汇总（跳过非 dict 的值，如顶层汇总字段）
        model_results = [r for r in result.values() if isinstance(r, dict)]
        total_hit = sum(r.get("hit_count", 0) for r in model_results)
        total_queries = sum(r.get("total_queries", 0) for r in model_results)
        total_tokens = sum(r.get("total_tokens", 0) for r in model_results)
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
                if isinstance(stats, dict)
            },
            "updated_at": task.get("updated_at", ""),
        }

        written += 1

    if written > 0:
        existing.update({
            "date": date_label,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scenarios": scenarios,
        })
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f"[daily_report] 写入 {filepath}，新增 {written} 个场景")

    return written


def find_daily_overview_tasks(target_date: str) -> List[dict]:
    """
    查找指定日期的所有全场景各模型任务（支持全天 + 时段限定任务）。
    target_date: "04-01" 格式

    匹配规则：
    - "04-11_全场景_各模型" → 全天任务
    - "04-11_10~11_全场景_各模型" → 时段限定任务
    - "【v9】04-0804-08_全场景_各模型" → 带前缀的全天任务
    """
    tasks = find_daily_tasks(target_date)
    overview_tasks = []
    for task in tasks:
        task_name = task.get("task_name", "")
        if "全场景_各模型" in task_name:
            overview_tasks.append(task)
    return overview_tasks


def format_report(task: dict, target_date: str, detail_url: str, task_id: str = "") -> str:
    """
    格式化报告内容（Markdown）
    """
    result = task.get("result", {})
    query = task.get("query", {})
    task_name = task.get("task_name", "")

    # 数据日期（只取年月日）
    start_dt = query.get("start_datetime", "")
    data_date = start_dt.split(" ")[0] if start_dt else target_date

    # 提取时段（如 "10~11"），区分全天任务和时段限定任务
    time_segment = ""
    m = re.search(r"(\d{2}~\d{2})", task_name)
    if m:
        time_segment = f" ({m.group(1)}时段)"

    # 汇总整体命中率
    total_hit = 0
    total_queries = 0
    total_tokens = 0
    for stats in result.values():
        if not isinstance(stats, dict):
            continue
        total_hit += stats.get("hit_count", 0)
        total_queries += stats.get("total_queries", 0)
        total_tokens += stats.get("total_tokens", 0)
    overall_rate = (total_hit / total_queries * 100) if total_queries > 0 else 0

    # 构建各模型明细
    model_lines = []
    for model, stats in sorted(result.items(), key=lambda x: x[1].get("hit_rate_percent", 0) if isinstance(x[1], dict) else 0, reverse=True):
        if not isinstance(stats, dict):
            continue
        rate = stats.get("hit_rate_percent", 0)
        hit = stats.get("hit_count", 0)
        total = stats.get("total_queries", 0)
        tokens = stats.get("total_tokens", 0)

        model_lines.append(
            f"> 🤖 **{model}**: <font color=\"green\">{rate:.1f}%</font>  "
            f"命中 {hit:,} / {total:,}  tokens {tokens:,}"
        )

    overall_color = "green" if overall_rate >= 50 else ("orange" if overall_rate >= 20 else "red")

    content = f"""##### 📊 KV Cache 模拟命中日报 (🗓️{data_date}{time_segment})

**整体命中率 🎯**: <font color="{overall_color}">{overall_rate:.1f}%</font>  命中 {total_hit:,} / {total_queries:,} 次  tokens {total_tokens:,}

**各模型命中率 🎯**
{chr(10).join(model_lines)}

[✨✨ 查看明细数据 ✨✨]({detail_url}/{task_id})"""

    return content


def send_im_notification(content: str, bot_url: str, bot_toid: list) -> bool:
    """发送 IM 通知"""
    payload = {
        "message": {
            "header": {"toid": bot_toid},
            "body": [{"type": "MD", "content": content}]
        }
    }

    try:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            bot_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[daily_report] IM 推送成功: HTTP {resp.status}")
            return True
    except Exception as e:
        print(f"[daily_report] IM 推送失败: {e}")
        return False


def main():
    # 计算昨天的日期
    yesterday = datetime.now() - timedelta(days=1)
    target_date = yesterday.strftime("%m-%d")  # "04-01" 格式

    print(f"[daily_report] 查找 {target_date} 的已完成任务...")

    # 查找所有已完成任务并写入 daily_reports/
    tasks = find_daily_tasks(target_date)
    if tasks:
        print(f"[daily_report] 找到 {len(tasks)} 个已完成任务")
        save_daily_report(target_date, tasks)
    else:
        print(f"[daily_report] 未找到 {target_date} 的已完成任务")

    # 查找全场景任务推送 IM（支持全天 + 时段限定任务）
    overview_tasks = find_daily_overview_tasks(target_date)
    if not overview_tasks:
        print(f"[daily_report] 未找到 {target_date} 的全场景任务，跳过推送")
        return

    print(f"[daily_report] 找到 {len(overview_tasks)} 个全场景任务")

    # 加载配置（日报专用配置，fallback 到通用配置）
    config = load_config()
    bot_url = config.get("daily_report_im_bot_url") or config.get("notify_im_bot_url", "")
    bot_toid = config.get("daily_report_im_bot_toid") or config.get("notify_im_bot_toid", [])
    detail_url = config.get("daily_report_detail_url", "https://vortex.n.baidu-int.com/olap/discovery")

    if not bot_url or not bot_toid:
        print("[daily_report] 未配置 IM bot，跳过推送")
        return

    # 逐个推送各时段的全场景报告
    for task in overview_tasks:
        task_name = task.get("task_name", "")
        task_id = task.get("task_id", "")
        print(f"[daily_report] 推送任务: {task_name}")

        content = format_report(task, target_date, detail_url, task_id)
        print("[daily_report] 报告内容:")
        print(content)
        print()

        send_im_notification(content, bot_url, bot_toid)


if __name__ == "__main__":
    main()

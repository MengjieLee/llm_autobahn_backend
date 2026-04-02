#!/usr/bin/env python3
"""
每日 KV Cache 命中率报告推送脚本

功能：
- 查找前一天的 "{mm-dd}_全场景_各模型" 任务
- 汇总各模型命中率数据
- 推送到 IM 机器人群

用法：
- 手动执行: python scripts/daily_report.py
- crontab:  0 10 * * * cd /path/to/backend && python scripts/daily_report.py

配置读取 olap_config.json 中的:
- notify_im_bot_url
- notify_im_bot_toid
"""

import json
import glob
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent
STATUS_DIR = BASE_DIR / "olap_database" / "status"
CONFIG_FILE = BASE_DIR / "app" / "conf" / "olap_config.json"


def load_config():
    """加载 olap 配置"""
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


from typing import Optional


def find_daily_overview_task(target_date: str) -> Optional[dict]:
    """
    查找指定日期的全场景各模型任务
    target_date: "04-01" 格式
    """
    pattern = f"*{target_date}_*全场景_各模型*"

    # 遍历所有用户目录
    for status_file in STATUS_DIR.glob(f"**/*.json"):
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            task_name = data.get("task_name", "")
            # 匹配 "{mm-dd}_全场景_各模型" 模式
            if target_date in task_name and "全场景_各模型" in task_name:
                stage = data.get("pipeline", {}).get("current_stage", "")
                if stage == "done" and data.get("result"):
                    return data
        except Exception:
            continue

    return None


def format_report(task: dict, target_date: str, detail_url: str) -> str:
    """
    格式化报告内容（Markdown）
    """
    result = task.get("result", {})
    query = task.get("query", {})

    # 数据日期（只取年月日）
    start_dt = query.get("start_datetime", "")
    data_date = start_dt.split(" ")[0] if start_dt else target_date

    # 构建各模型明细
    model_lines = []
    for model, stats in sorted(result.items(), key=lambda x: x[1].get("hit_rate_percent", 0), reverse=True):
        rate = stats.get("hit_rate_percent", 0)
        hit = stats.get("hit_count", 0)
        total = stats.get("total_queries", 0)
        tokens = stats.get("total_tokens", 0)

        model_lines.append(
            f"> **{model}**: <font color=\"green\">{rate:.1f}%</font>  "
            f"命中 {hit:,} / {total:,}  tokens {tokens:,}"
        )

    content = f"""##### KV Cache 模拟命中日报 📊 (🗓️{data_date})

**各模型命中率 🎯**
{chr(10).join(model_lines)}

[✨✨ 查看明细数据]({detail_url}) ✨✨"""

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

    print(f"[daily_report] 查找 {target_date} 的全场景各模型任务...")

    # 查找任务
    task = find_daily_overview_task(target_date)

    if not task:
        print(f"[daily_report] 未找到 {target_date} 的已完成任务，跳过推送")
        return

    print(f"[daily_report] 找到任务: {task.get('task_name')}")

    # 加载配置（日报专用配置，fallback 到通用配置）
    config = load_config()
    bot_url = config.get("daily_report_im_bot_url") or config.get("notify_im_bot_url", "")
    bot_toid = config.get("daily_report_im_bot_toid") or config.get("notify_im_bot_toid", [])
    detail_url = config.get("daily_report_detail_url", "https://vortex.n.baidu-int.com/olap/kv")

    if not bot_url or not bot_toid:
        print("[daily_report] 未配置 IM bot，跳过推送")
        return

    # 格式化报告
    content = format_report(task, target_date, detail_url)
    print("[daily_report] 报告内容:")
    print(content)
    print()

    # 发送通知
    send_im_notification(content, bot_url, bot_toid)


if __name__ == "__main__":
    main()

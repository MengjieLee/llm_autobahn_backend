#!/usr/bin/env python3
"""
将 ES 查询结果中的请求转换为 input_ids 序列

特性:
- 根据请求中的 model 自动选择对应的 HuggingFace tokenizer
- 使用 tokenizer 自带的 chat_template，支持 tools calling
- 对 messages 进行深度清洗 (tool_calls/arguments 字符串化修复)

用法:
    python tokenize_script.py --input /path/to/input.json --output /path/to/output.json
"""

import json
import re
import os
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime

# HuggingFace Token 从环境变量读取（在 .env 中配置 HF_TOKEN）
os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "true")


# ============================================================
# 模型到 Tokenizer 的映射配置
# ============================================================
MODEL_TOKENIZER_MAPPING = {
    "kimi-k2.5": "moonshotai/Kimi-K2.5",
    "glm-5": "zai-org/GLM-5",
    "minimax-m2.5": "MiniMaxAI/MiniMax-M2.5",
    "deepseek-v3.2": "deepseek-ai/DeepSeek-V3.2",
    "glm-4.7": "zai-org/GLM-4.7",
    "minimax-m2.1": "MiniMaxAI/MiniMax-M2.1",
    "_default": "zai-org/GLM-5",
}


# ============================================================
# 消息清洗
# ============================================================
def sanitize_message(msg):
    """
    深度清洗单条消息，确保所有嵌套的 JSON 结构都是对象而非字符串。
    重点处理 tool_calls 和 arguments。
    """
    if not isinstance(msg, dict):
        return msg

    if "tool_calls" in msg:
        tool_calls = msg["tool_calls"]

        # tool_calls 整体被存成了字符串 -> 解析
        if isinstance(tool_calls, str):
            try:
                msg["tool_calls"] = json.loads(tool_calls)
                tool_calls = msg["tool_calls"]
            except json.JSONDecodeError:
                pass

        # tool_calls 是列表，遍历内部结构
        if isinstance(tool_calls, list):
            for tool_call in tool_calls:
                if not isinstance(tool_call, dict):
                    continue

                func = tool_call.get("function")
                if isinstance(func, str):
                    try:
                        tool_call["function"] = json.loads(func)
                        func = tool_call["function"]
                    except (json.JSONDecodeError, Exception):
                        pass

                # arguments 字段可能被序列化为字符串
                if isinstance(func, dict) and "arguments" in func:
                    args = func["arguments"]
                    if isinstance(args, str):
                        try:
                            func["arguments"] = json.loads(args)
                        except json.JSONDecodeError:
                            pass

    return msg


# ============================================================
# TokenizerManager
# ============================================================
class TokenizerManager:
    """Tokenizer 管理器，支持缓存和自动选择"""

    def __init__(self):
        self._cache = {}

    def get_tokenizer_config(self, model_name: str) -> str:
        """根据模型名获取 tokenizer 配置"""
        if model_name in MODEL_TOKENIZER_MAPPING:
            return MODEL_TOKENIZER_MAPPING[model_name]

        model_lower = model_name.lower()
        for key, value in MODEL_TOKENIZER_MAPPING.items():
            if key != "_default" and model_lower.startswith(key.lower()):
                return value

        return MODEL_TOKENIZER_MAPPING["_default"]

    def get_tokenizer(self, model_name: str):
        """获取或创建 tokenizer (带缓存)"""
        config = self.get_tokenizer_config(model_name)

        if config in self._cache:
            return self._cache[config], config

        tokenizer = self._create_tokenizer(config)
        self._cache[config] = tokenizer
        return tokenizer, config

    def _create_tokenizer(self, config: str):
        """创建 transformers tokenizer"""
        import warnings
        warnings.filterwarnings("ignore", message=".*rope_parameters.*")
        warnings.filterwarnings("ignore", message=".*Token indices sequence length.*")

        from transformers import AutoTokenizer
        return AutoTokenizer.from_pretrained(config, trust_remote_code=True)


# ============================================================
# 辅助函数
# ============================================================
def extract_qianfan_model(raw_str: str) -> Optional[str]:
    """从 @raw 字符串中提取 qianfan_model 的值"""
    try:
        match = re.search(r'qianfan_model:([a-zA-Z0-9._-]+)', raw_str)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


def extract_request_body(raw_str: str) -> Optional[Dict]:
    """从 @raw 字段中提取请求 body"""
    try:
        match = re.search(r'body:(\{.*\}), rawBodyLength:', raw_str)
        if match:
            body_str = match.group(1)
            return json.loads(body_str)
    except (json.JSONDecodeError, AttributeError):
        pass
    return None


def apply_chat_template(tokenizer, messages: List[Dict], tools: Optional[List]) -> List[int]:
    """
    使用 tokenizer 的 chat_template 将 messages 转换为 input_ids

    使用 return_tensors=None 直接返回 Python list，无需 PyTorch。
    兼容多种返回类型: list, dict, Tensor, Encoding
    """
    # 尝试带 tools 的 apply_chat_template
    if tools:
        try:
            raw_ret = tokenizer.apply_chat_template(
                conversation=messages,
                tools=tools,
                add_generation_prompt=True,
                tokenize=True,
                return_tensors=None,
            )
            return _extract_input_ids(raw_ret)
        except TypeError:
            # 某些 tokenizer 不支持 tools 参数
            pass

    # 不带 tools
    raw_ret = tokenizer.apply_chat_template(
        conversation=messages,
        add_generation_prompt=True,
        tokenize=True,
        return_tensors=None,
    )
    return _extract_input_ids(raw_ret)


def _extract_input_ids(raw_ret) -> List[int]:
    """从 apply_chat_template 的返回值中提取 input_ids 列表"""
    # dict / BatchEncoding -> 取 input_ids 字段
    if hasattr(raw_ret, '__getitem__') and not isinstance(raw_ret, (list, tuple)):
        try:
            raw_ret = raw_ret["input_ids"]
        except (KeyError, TypeError, IndexError):
            pass

    # Tensor -> 转 list
    if hasattr(raw_ret, 'tolist'):
        ids = raw_ret.tolist()
        if ids and isinstance(ids[0], list):
            ids = ids[0]
        return [int(x) for x in ids]

    # 列表 (return_tensors=None 的正常返回)
    if isinstance(raw_ret, list):
        ids = raw_ret
        if ids and isinstance(ids[0], list):
            ids = ids[0]
        return [int(x) for x in ids]

    # Encoding 对象
    if hasattr(raw_ret, 'ids'):
        return [int(x) for x in raw_ret.ids]

    # 兜底
    return [int(x) for x in list(raw_ret)]


# ============================================================
# 单条记录转换
# ============================================================
def convert_record(
    record: Dict,
    tokenizer_manager: TokenizerManager,
    override_tokenizer: Optional[str] = None,
    default_model: str = "glm-5",
    verbose: bool = False
) -> Optional[Dict[str, Any]]:
    """
    转换单条记录

    模型选择优先级：
    1. override_tokenizer (命令行参数强制指定)
    2. @raw 中的 qianfan_model 值
    3. body 中的 model 字段
    4. default_model 默认模型
    """
    try:
        source = record.get("_source", {})
        raw_str = source.get("@raw", "")
        as_id = source.get("as_id", "")
        timestamp = source.get("@timestamp", "")

        body = extract_request_body(raw_str)
        if not body:
            if verbose:
                print(f"[WARN] 无法提取 body: as_id={as_id}")
            return None

        messages = body.get("messages", [])
        if not messages:
            if verbose:
                print(f"[WARN] messages 为空: as_id={as_id}")
            return None

        # 深度清洗 messages
        valid_messages = []
        for msg in messages:
            if isinstance(msg, dict):
                valid_messages.append(sanitize_message(msg))
            elif isinstance(msg, str):
                valid_messages.append({"role": "user", "content": msg})

        if not valid_messages:
            if verbose:
                print(f"[WARN] 清洗后无有效消息: as_id={as_id}")
            return None

        tools = body.get("tools") or None

        # 提取模型名，优先级: override > qianfan_model > body.model > default
        qianfan_model = extract_qianfan_model(raw_str)
        body_model = body.get("model", "")

        if override_tokenizer:
            model_for_tokenizer = override_tokenizer
        elif qianfan_model:
            model_for_tokenizer = qianfan_model
        elif body_model:
            model_for_tokenizer = body_model
        else:
            model_for_tokenizer = default_model

        # 获取对应的 tokenizer
        tokenizer, tokenizer_config = tokenizer_manager.get_tokenizer(model_for_tokenizer)

        # 应用 chat_template
        input_ids = apply_chat_template(tokenizer, valid_messages, tools)

        return {
            "as_id": as_id,
            "timestamp": timestamp,
            "qianfan_model": qianfan_model,
            "body_model": body_model,
            "model_used": model_for_tokenizer,
            "tokenizer_used": tokenizer_config,
            "has_tools": tools is not None and len(tools) > 0,
            "tools_count": len(tools) if tools else 0,
            "messages_count": len(valid_messages),
            "input_ids": input_ids,
            "input_ids_length": len(input_ids),
        }
    except Exception as e:
        if verbose:
            import traceback
            traceback.print_exc()
        return None


# ============================================================
# main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="将 ES 查询结果转换为 input_ids (自动选择 tokenizer，按 model 分桶输出)")
    parser.add_argument("--input", "-i", required=True, help="输入文件路径 (JSONL)")
    parser.add_argument("--output-dir", "-o", required=True, help="输出目录，按 model 生成多个文件")
    parser.add_argument("--file-prefix", "-p", default="",
                        help="输出文件名前缀 (如 kv_20260325_000000_20260325_010000)")
    parser.add_argument("--override-tokenizer", "-t", default=None,
                        help="强制使用指定 tokenizer (覆盖自动选择)")
    parser.add_argument("--default-model", "-d", default="glm-5",
                        help="默认模型 (当 qianfan_model 和 body.model 都不存在时使用，默认 glm-5)")
    parser.add_argument("--limit", "-l", type=int, default=0,
                        help="限制处理记录数 (0=不限制)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="显示详细错误信息")
    parser.add_argument("--show-mapping", action="store_true",
                        help="显示模型到 tokenizer 的映射配置")

    args = parser.parse_args()

    if args.show_mapping:
        print("[INFO] 模型到 Tokenizer 的映射配置:")
        for model, tokenizer in MODEL_TOKENIZER_MAPPING.items():
            print(f"  {model} -> {tokenizer}")
        return 0

    if not os.path.exists(args.input):
        print(f"[ERROR] 输入文件不存在: {args.input}")
        return 1

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    # 从输入文件名推导前缀（如未指定）
    file_prefix = args.file_prefix
    if not file_prefix:
        file_prefix = os.path.splitext(os.path.basename(args.input))[0]

    print(f"[INFO] 开始处理...")
    print(f"[INFO] 输入文件: {args.input}")
    print(f"[INFO] 输出目录: {output_dir}")
    print(f"[INFO] 文件前缀: {file_prefix}")
    if args.override_tokenizer:
        print(f"[INFO] 强制 Tokenizer: {args.override_tokenizer}")
    else:
        print(f"[INFO] Tokenizer: 自动选择 (优先 qianfan_model, 其次 body.model)")
    print(f"[INFO] 默认模型: {args.default_model}")

    tokenizer_manager = TokenizerManager()

    # 统计信息
    model_stats = {}
    tokenizer_stats = {}
    success_count = 0
    failed_count = 0
    total_records = 0
    start_time = datetime.now()

    # 流式处理：逐行读 JSONL → 转换 → 按 model 实时写入文件
    # 不在内存中攒 records 或 model_results，避免大文件 OOM
    model_files = {}       # model -> {"fh": file_handle, "incomplete": path, "final": path, "count": int}
    output_files = {}      # model -> final_file_path
    model_counts = {}      # model -> count

    print(f"[INFO] 流式处理中...")
    with open(args.input, 'r', encoding='utf-8') as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            total_records += 1
            if args.limit > 0 and total_records > args.limit:
                break

            record = json.loads(line)
            result = convert_record(
                record, tokenizer_manager, args.override_tokenizer,
                args.default_model, args.verbose
            )

            if result:
                success_count += 1
                model = result.get("model_used", "unknown")
                model_stats[model] = model_stats.get(model, 0) + 1
                tokenizer_used = result.get("tokenizer_used", "unknown")
                tokenizer_stats[tokenizer_used] = tokenizer_stats.get(tokenizer_used, 0) + 1

                # 按需打开 model 文件句柄（懒初始化）
                if model not in model_files:
                    final_file = os.path.join(output_dir, f"{file_prefix}_{model}_input_ids.json")
                    incomplete_file = final_file + ".incomplete"
                    fh = open(incomplete_file, 'w', encoding='utf-8')
                    fh.write('[\n')
                    model_files[model] = {
                        "fh": fh, "incomplete": incomplete_file,
                        "final": final_file, "count": 0
                    }

                mf = model_files[model]
                if mf["count"] > 0:
                    mf["fh"].write(',\n')
                json.dump(result, mf["fh"], ensure_ascii=False)
                mf["count"] += 1
                model_counts[model] = mf["count"]
            else:
                failed_count += 1

            processed = success_count + failed_count
            if processed % 10000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                speed = processed / elapsed if elapsed > 0 else 0
                print(f"[INFO] 进度: {processed}/{total_records}+, "
                      f"成功: {success_count}, 失败: {failed_count}, 速度: {speed:.0f} 条/秒")

    # 关闭所有文件句柄，完成 .incomplete → .json rename
    for model, mf in model_files.items():
        mf["fh"].write('\n]')
        mf["fh"].close()
        os.rename(mf["incomplete"], mf["final"])
        output_files[model] = mf["final"]
        print(f"[INFO] 模型 {model}: {mf['count']} 条 -> {mf['final']}")

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n[INFO] ========== 处理完成 ==========")
    print(f"[INFO] 总记录: {total_records}, 成功: {success_count}, 失败: {failed_count}")
    print(f"[INFO] 模型数: {len(model_files)}")
    print(f"[INFO] 耗时: {elapsed:.1f} 秒")

    print(f"\n[INFO] ========== 模型分布 ==========")
    for model, count in sorted(model_stats.items(), key=lambda x: -x[1]):
        pct = count / success_count * 100 if success_count > 0 else 0
        print(f"  {model}: {count} 条 ({pct:.1f}%)")

    print(f"\n[INFO] ========== Tokenizer 使用 ==========")
    for tok, count in sorted(tokenizer_stats.items(), key=lambda x: -x[1]):
        print(f"  {tok}: {count} 条")

    # 输出 JSON 汇总供上层脚本解析
    summary = {
        "models": {m: {"count": model_counts.get(m, 0), "file": output_files[m]} for m in output_files},
        "success_count": success_count,
        "failed_count": failed_count,
    }
    print(f"\n[SUMMARY] {json.dumps(summary, ensure_ascii=False)}")

    return 0


if __name__ == "__main__":
    exit(main())

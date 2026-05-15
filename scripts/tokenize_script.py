#!/usr/bin/env python3
"""
将 ES 查询结果中的请求转换为 input_ids 序列

特性:
- 根据请求中的 model 自动选择对应的 HuggingFace tokenizer
- 使用 tokenizer 自带的 chat_template，支持 tools calling
- 对 messages 进行深度清洗 (tool_calls/arguments 字符串化修复)
- 多进程并行 tokenize（CPU 密集部分）
- 直接输出 cache_calc 需要的 txt 格式，无需中间 JSON

用法:
    python tokenize_script.py --input /path/to/input.jsonl --output-dir /path/to/output/
"""

import json
import re
import os
import argparse
from multiprocessing import Pool, cpu_count
from typing import List, Dict, Any, Optional
from datetime import datetime

# HuggingFace Token 从环境变量读取（在 .env 中配置 HF_TOKEN）
os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


# ============================================================
# 模型到 Tokenizer 的映射配置
# ============================================================
MODEL_TOKENIZER_MAPPING = {
    "kimi-k2.5": "moonshotai/Kimi-K2.5",
    "glm-5.1": "zai-org/GLM-5.1",
    "glm-5": "zai-org/GLM-5",
    "minimax-m2.5": "MiniMaxAI/MiniMax-M2.5",
    "deepseek-v3.2": "deepseek-ai/DeepSeek-V3.2",
    "deepseek-v4-flash": "deepseek-ai/DeepSeek-V4-Flash",
    "deepseek-v4-pro": "deepseek-ai/DeepSeek-V4-Pro",
    "glm-4.7": "zai-org/GLM-4.7",
    "minimax-m2.1": "MiniMaxAI/MiniMax-M2.1",
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

    def get_tokenizer_config(self, model_name: str) -> Optional[str]:
        """根据模型名获取 tokenizer 配置，无匹配返回 None"""
        if model_name in MODEL_TOKENIZER_MAPPING:
            return MODEL_TOKENIZER_MAPPING[model_name]

        model_lower = model_name.lower()
        for key, value in MODEL_TOKENIZER_MAPPING.items():
            if model_lower.startswith(key.lower()):
                return value

        return None

    def get_tokenizer(self, model_name: str):
        """获取或创建 tokenizer (带缓存)，无匹配返回 (None, None)"""
        config = self.get_tokenizer_config(model_name)
        if config is None:
            return None, None

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
        warnings.filterwarnings("ignore", message=".*is not supported and can yield errors.*")

        from transformers import AutoTokenizer
        try:
            tok = AutoTokenizer.from_pretrained(config, trust_remote_code=True)
        except (ValueError, AttributeError, OSError):
            # 部分新模型（如 deepseek_v32）的 config.json 含有当前 transformers
            # 版本不认识的架构字段，导致 AutoConfig 失败。
            # 回退：用 PreTrainedTokenizerFast 直接加载 tokenizer 文件，跳过模型 config。
            from transformers import PreTrainedTokenizerFast
            tok = PreTrainedTokenizerFast.from_pretrained(config, trust_remote_code=True)

        # PreTrainedTokenizerFast fallback 可能丢失 chat_template（跳过了 model config），
        # 手动从 tokenizer_config.json 中读取补上。
        if not getattr(tok, "chat_template", None):
            tok.chat_template = self._load_chat_template_from_config(config)
        return tok

    @staticmethod
    def _load_chat_template_from_config(config: str):
        """从 HF cache 中的 tokenizer_config.json 读取 chat_template"""
        try:
            from huggingface_hub import hf_hub_download
            import json as _json
            path = hf_hub_download(config, "tokenizer_config.json")
            with open(path, "r", encoding="utf-8") as f:
                data = _json.load(f)
            tpl = data.get("chat_template")
            if tpl:
                return tpl
        except Exception:
            pass
        # 兜底：DeepSeek V3 标准 chat template
        return (
            "{% for message in messages %}"
            "{% if message['role'] == 'system' %}"
            "<|begin▁of▁sentence|>{{ message['content'] }}"
            "{% elif message['role'] == 'user' %}"
            "<|User|>{{ message['content'] }}"
            "{% elif message['role'] == 'assistant' %}"
            "<|Assistant|>{{ message['content'] }}"
            "{% endif %}"
            "{% endfor %}"
            "{% if add_generation_prompt %}<|Assistant|>{% endif %}"
        )


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
            import io, sys as _sys
            _backup = _sys.stderr
            _sys.stderr = io.StringIO()  # 屏蔽 "Failed to convert tools" 噪音
            try:
                raw_ret = tokenizer.apply_chat_template(
                    conversation=messages,
                    tools=tools,
                    add_generation_prompt=True,
                    tokenize=True,
                    return_tensors=None,
                )
            finally:
                _sys.stderr = _backup
            return _extract_input_ids(raw_ret)
        except (TypeError, ValueError, KeyError, Exception):
            # tools 转换失败（不支持 tools / JSON Schema 不完整等），回退到不带 tools
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
# 单条记录转换（纯函数，用于多进程）
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
    无匹配的模型跳过（返回 None），不再兜底到 default_model
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

        # 提取模型名，优先级: override > qianfan_model
        qianfan_model = extract_qianfan_model(raw_str)
        body_model = body.get("model", "")

        if override_tokenizer:
            model_for_tokenizer = override_tokenizer
        elif qianfan_model:
            model_for_tokenizer = qianfan_model
        else:
            # 无法确定模型，跳过
            if verbose:
                print(f"[WARN] 无法确定模型 (qianfan_model 为空): as_id={as_id}")
            return None

        # 获取对应的 tokenizer，无匹配则跳过（不写入空 input_ids，结果不准）
        tokenizer, tokenizer_config = tokenizer_manager.get_tokenizer(model_for_tokenizer)
        if tokenizer is None:
            if verbose:
                print(f"[WARN] 无匹配 tokenizer，跳过: model={model_for_tokenizer}, as_id={as_id}")
            return None

        # 超长文本预检：估算总字符数，超过 300K token 等价字符量则跳过
        # 300K tokens × 4 字符/token = 1,200,000 字符（保守估算）
        # 目的：避免超长请求拖慢 tokenize（单条 300K+ token 耗时约 150× 正常记录）
        # 这类记录在生产中无法命中 KV cache（超出上下文窗口），跳过不影响命中率统计
        _SKIP_CHARS_THRESHOLD = 1_200_000  # 300K tokens × 4 chars/token
        total_content_chars = sum(
            len(str(m.get("content") or "")) for m in valid_messages if isinstance(m, dict)
        )
        if total_content_chars > _SKIP_CHARS_THRESHOLD:
            if verbose:
                print(f"[SKIP] 文本过长（~{total_content_chars // 1000}K 字符"
                      f"，阈值=1200K 字符≈300K tokens），跳过: as_id={as_id}")
            return "TOO_LONG"  # 特殊标记：与 None（解析失败）区分，用于统计

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
# 多进程 worker
# ============================================================
_worker_tm = None   # 每个 worker 进程内的 TokenizerManager
_worker_override = None
_worker_default = None
_worker_verbose = False

# daemon 主进程预加载后写入此变量，fork 出的 worker 通过 COW 继承，避免重复加载
_preloaded_tm: Optional["TokenizerManager"] = None


def _worker_init(override_tokenizer, default_model, verbose):
    """多进程 worker 初始化：优先继承主进程预加载的 TokenizerManager（COW 共享）"""
    global _worker_tm, _worker_override, _worker_default, _worker_verbose, _preloaded_tm
    _worker_tm = _preloaded_tm if _preloaded_tm is not None else TokenizerManager()
    _worker_override = override_tokenizer
    _worker_default = default_model
    _worker_verbose = verbose


def _worker_process_batch(batch, model_filter=None):
    """
    处理一个 batch 的记录。
    batch: list of (line_idx, json_line_str)
    model_filter: set of model names to keep; None = keep all

    返回: (results, too_long_count)
      results: list of (line_idx, model, txt_line) 或 (line_idx, None, None) 表示失败/跳过
      too_long_count: 本 batch 中因超长（>300K tokens）被跳过的记录数
    """
    results = []
    too_long_count = 0
    for line_idx, json_str in batch:
        # 快速预过滤：在 JSON 解析和 tokenize 之前，通过 regex 提取 qianfan_model
        # 若不在 model_filter 中则直接跳过，避免无效 tokenize（最大优化点）
        if model_filter:
            m = re.search(r'qianfan_model:([a-zA-Z0-9._-]+)', json_str)
            if m and m.group(1) not in model_filter:
                results.append((line_idx, None, None))
                continue
            # 若未提取到 qianfan_model，透传给 convert_record 正常处理（会 return None）

        try:
            record = json.loads(json_str)
        except json.JSONDecodeError:
            results.append((line_idx, None, None))
            continue

        result = convert_record(
            record, _worker_tm, _worker_override, _worker_default, _worker_verbose
        )
        if result == "TOO_LONG":
            too_long_count += 1
            results.append((line_idx, None, None))
        elif result:
            model = result["model_used"]
            ids_str = " ".join(map(str, result["input_ids"]))
            txt_line = f"'input_ids': [{ids_str}]"
            results.append((line_idx, model, txt_line))
        else:
            results.append((line_idx, None, None))
    return results, too_long_count


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
    parser.add_argument("--workers", "-W", type=int, default=0,
                        help="tokenize 并行 worker 数 (0=自动，基于 CPU 核数)")
    parser.add_argument("--batch-size", "-B", type=int, default=200,
                        help="每个 batch 提交给 worker 的记录数 (默认 200)")

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

    # worker 数量
    num_workers = args.workers if args.workers > 0 else max(1, min(cpu_count() - 1, 4))
    batch_size = args.batch_size
    max_pending_batches = num_workers * 2

    print(f"[INFO] 开始处理...")
    print(f"[INFO] 输入文件: {args.input}")
    print(f"[INFO] 输出目录: {output_dir}")
    print(f"[INFO] 文件前缀: {file_prefix}")
    if args.override_tokenizer:
        print(f"[INFO] 强制 Tokenizer: {args.override_tokenizer}")
    else:
        print(f"[INFO] Tokenizer: 自动选择 (优先 qianfan_model, 其次 body.model)")
    print(f"[INFO] 默认模型: {args.default_model}")
    print(f"[INFO] 并行 workers: {num_workers}, batch_size: {batch_size}")

    # 统计信息
    model_stats = {}
    tokenizer_stats = {}
    success_count = 0
    failed_count = 0
    too_long_count = 0
    total_records = 0
    start_time = datetime.now()

    # 输出文件句柄：model -> {fh, incomplete, final, count}
    # 直接输出 txt 格式（方案 B：消除中间 JSON）
    model_files = {}
    output_files = {}
    model_counts = {}

    print(f"[INFO] 流式处理中...")

    # ---- 方案 A+B：多进程并行 + 直接输出 txt ----
    # 主进程读 JSONL 攒 batch → worker pool 并行 tokenize
    # → 主进程按原始行号排序写出（保持时间序）
    pool = Pool(
        processes=num_workers,
        initializer=_worker_init,
        initargs=(args.override_tokenizer, args.default_model, args.verbose),
    )

    pending_futures = []    # (future, batch_start_idx, batch_size)
    current_batch = []      # [(line_idx, json_line_str), ...]
    line_idx = 0

    def _flush_results():
        """收集所有已完成的 futures 并写出结果"""
        nonlocal success_count, failed_count, too_long_count
        for future in pending_futures:
            batch_results, batch_too_long = future.get()
            too_long_count += batch_too_long
            # batch_results 已经按提交顺序返回（同一 batch 内有序）
            for idx, model, txt_line in batch_results:
                if model is not None:
                    success_count += 1
                    model_stats[model] = model_stats.get(model, 0) + 1

                    if model not in model_files:
                        final_file = os.path.join(output_dir, f"{file_prefix}_{model}_input_ids.txt")
                        incomplete_file = final_file + ".incomplete"
                        fh = open(incomplete_file, 'w', encoding='utf-8')
                        model_files[model] = {
                            "fh": fh, "incomplete": incomplete_file,
                            "final": final_file, "count": 0
                        }

                    mf = model_files[model]
                    mf["fh"].write(txt_line + '\n')
                    mf["count"] += 1
                    model_counts[model] = mf["count"]
                else:
                    failed_count += 1
        pending_futures.clear()

    try:
        with open(args.input, 'r', encoding='utf-8') as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue

                total_records += 1
                if args.limit > 0 and total_records > args.limit:
                    break

                current_batch.append((line_idx, line))
                line_idx += 1

                if len(current_batch) >= batch_size:
                    future = pool.apply_async(_worker_process_batch, (current_batch,))
                    pending_futures.append(future)
                    current_batch = []

                    if len(pending_futures) >= max_pending_batches:
                        _flush_results()

                        processed = success_count + failed_count
                        if processed > 0 and processed % 10000 < batch_size:
                            elapsed = (datetime.now() - start_time).total_seconds()
                            speed = processed / elapsed if elapsed > 0 else 0
                            print(f"[INFO] 进度: {processed}/{total_records}+, "
                                  f"成功: {success_count}, 失败: {failed_count}, 速度: {speed:.0f} 条/秒")

        # 提交最后一个不完整的 batch
        if current_batch:
            future = pool.apply_async(_worker_process_batch, (current_batch,))
            pending_futures.append(future)

        # flush 所有剩余结果
        _flush_results()

    finally:
        pool.close()
        pool.join()

    # 关闭写入，rename incomplete → final
    for model, mf in model_files.items():
        mf["fh"].close()
        os.rename(mf["incomplete"], mf["final"])
        output_files[model] = mf["final"]
        print(f"[INFO] 模型 {model}: {mf['count']} 条 -> {mf['final']}")

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n[INFO] ========== 处理完成 ==========")
    print(f"[INFO] 总记录: {total_records}, 成功: {success_count}, 失败: {failed_count}, 超长跳过: {too_long_count}")
    print(f"[INFO] 模型数: {len(model_files)}")
    print(f"[INFO] 耗时: {elapsed:.1f} 秒")
    print(f"[INFO] Workers: {num_workers}, Batch: {batch_size}")

    print(f"\n[INFO] ========== 模型分布 ==========")
    for model, count in sorted(model_stats.items(), key=lambda x: -x[1]):
        pct = count / success_count * 100 if success_count > 0 else 0
        print(f"  {model}: {count} 条 ({pct:.1f}%)")

    # 输出 JSON 汇总供上层脚本解析
    summary = {
        "models": {m: {"count": model_counts.get(m, 0), "file": output_files[m]} for m in output_files},
        "success_count": success_count,
        "failed_count": failed_count,
        "too_long_count": too_long_count,
    }
    print(f"\n[SUMMARY] {json.dumps(summary, ensure_ascii=False)}")

    return 0


if __name__ == "__main__":
    exit(main())

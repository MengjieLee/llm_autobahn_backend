#!/usr/bin/env python3
"""
Preprocess last_agentic_10_split.jsonl into the text format expected by cache_cacl.cpp.

For each line in the JSONL file:
  1. Parse the JSON object which contains {"messages": [{"role": ..., "content": ...}, ...]}
  2. Concatenate all message contents into a single conversation string
  3. Tokenize with the provided tokenizer.json
  4. Output a line in the format: 'input_ids': [id1, id2, id3, ...]

Usage:
    python3 preprocess_jsonl.py \
        --input last_agentic_10_split.jsonl \
        --tokenizer tokenizer.json \
        --output input_ids.txt
"""

import argparse
import json
import os
from datetime import datetime
from tokenizers import Tokenizer


def build_conversation_text(messages):
    """Concatenate all messages in a conversation into a single string.

    Each message is formatted as:
        <role>: <content>
    Messages are joined by newlines to form a complete conversation.
    """
    parts = []
    for msg in messages:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        parts.append(f"{role}: {content}")
    return "\n".join(parts)


def normalize_timestamp(value):
    """Normalize timestamp to 'YYYY-MM-DD HH:MM:SS'."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Already normalized format
        if len(s) >= 19 and s[4] == "-" and s[7] == "-" and s[10] == " ":
            return s[:19]
        # ISO-8601 format, e.g. 2026-03-09T12:31:28.000Z
        iso = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(iso)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
        # numeric string timestamp
        try:
            return datetime.utcfromtimestamp(float(s)).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Convert JSONL conversation data to cache_cacl.cpp input format"
    )
    parser.add_argument(
        "--input", "-i",
        default="last_agentic_10_split.jsonl",
        help="Path to input JSONL file (default: last_agentic_10_split.jsonl)"
    )
    parser.add_argument(
        "--tokenizer", "-t",
        default="tokenizer.json",
        help="Path to tokenizer.json (default: tokenizer.json)"
    )
    parser.add_argument(
        "--output", "-o",
        default="input_ids.txt",
        help="Path to output file (default: input_ids.txt)"
    )
    parser.add_argument(
        "--sort-by-timestamp",
        action="store_true",
        help="Sort records by timestamp before writing output"
    )
    args = parser.parse_args()

    # Load tokenizer
    tokenizer = Tokenizer.from_file(args.tokenizer)

    total_lines = 0
    total_tokens = 0

    records = []
    with open(args.input, "r", encoding="utf-8") as fin:
        for line_no, line in enumerate(fin):
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            messages = data.get("messages", [])
            if not messages:
                continue
            ts = normalize_timestamp(data.get("timestamp"))
            records.append((ts, messages))
            if (line_no + 1) % 100 == 0:
                print(f"Processed {line_no + 1} lines...")

    if args.sort_by_timestamp:
        records.sort(key=lambda x: x[0] or "")

    with open(args.output, "w", encoding="utf-8") as fout:
        for ts, messages in records:
            conversation_text = build_conversation_text(messages)
            encoding = tokenizer.encode(conversation_text)
            token_ids = encoding.ids
            ids_str = ", ".join(str(tid) for tid in token_ids)
            if ts:
                fout.write(f"{ts} 'input_ids': [{ids_str}]\n")
            else:
                fout.write(f"'input_ids': [{ids_str}]\n")

            total_lines += 1
            total_tokens += len(token_ids)

    print(f"Done. Processed {total_lines} conversations, {total_tokens} total tokens.")
    print(f"Output written to: {args.output}")


if __name__ == "__main__":
    main()

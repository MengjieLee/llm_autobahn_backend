import logging
import os
import gc
import shutil
import asyncio
import threading
from typing import List, Dict, Any, Callable, Optional, Sequence
from datetime import datetime, timedelta
from .impl import ESIndexClient, _malloc_trim
from app.conf.config import settings


logger = logging.getLogger("es_query")

# 从 settings 读取配置
WINDOW_MINUTES = settings.ES_WINDOW_MINUTES

DEFAULT_APP_ID = settings.ES_DEFAULT_APP_ID
ES_HOST = settings.ES_HOST
ES_AUTH = (settings.ES_USER, settings.ES_PASSWORD)
ES_INDEX_PREFIX = settings.ES_INDEX_PREFIX

# 自适应降档：默认 60s，2GB 报错后依次降档
_WINDOW_SECONDS_TIERS = [60, 30, 15, 5]


def _is_2gb_error(exc: Exception) -> bool:
    """判断是否为 ES 2GB scroll buffer 超限错误"""
    msg = str(exc)
    return "2GB" in msg or "ReleasableBytesStreamOutput" in msg


def _is_scroll_limit_error(exc: Exception) -> bool:
    """判断是否为 ES scroll context 超限错误（集群级 500 限制）"""
    msg = str(exc)
    return "too many scroll contexts" in msg.lower() or "Trying to create too many scroll contexts" in msg


class ESIndexService:
    def __init__(self, date: str, app_id: str = DEFAULT_APP_ID, path: str = "",
                 models: Optional[Sequence[str]] = None):
        """
        :param date: 日期，格式 YYYY-MM-DD，用于确定索引名称
        :param app_id: 应用 ID，默认 app-3Lut8O2E
        :param path: 场景过滤路径，非空时添加 term filter
        :param models: 模型过滤列表，非空时按 api_name.keyword 精确匹配
        """
        self.date = date
        self.app_id = app_id
        self.path = path
        raw_models = models.split(",") if isinstance(models, str) else (models or [])
        self.models = [m.strip() for m in raw_models if m and m.strip()]
        if self.models:
            logger.info(f"[es_query] source model filter enabled: {len(self.models)} models")
        self.es = ESIndexClient(
            ES_HOST, ES_AUTH,
            f"{ES_INDEX_PREFIX}{self.date}"
        )
        self._extra_clients = []  # 跨日期创建的额外客户端

    def close(self):
        """关闭所有 ES 连接，释放连接池内存"""
        self.es.close()
        for c in self._extra_clients:
            c.close()
        self._extra_clients.clear()

    def _parse_datetime(self, time_str: str) -> datetime:
        """
        解析时间字符串，支持两种格式：
        - HH:MM:SS（使用 self.date 作为日期）
        - YYYY-MM-DD HH:MM:SS（完整日期时间）
        """
        if len(time_str) > 8:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        return datetime.strptime(f"{self.date} {time_str}", "%Y-%m-%d %H:%M:%S")

    def _to_utc(self, dt: datetime) -> str:
        """将北京时间 datetime 转换为 UTC 时间字符串"""
        utc_dt = dt - timedelta(hours=8)
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _get_es_for_date(self, date_str: str) -> ESIndexClient:
        """获取指定日期的 ES 客户端，跨日期时按需创建"""
        if date_str == self.date:
            return self.es
        client = ESIndexClient(
            ES_HOST, ES_AUTH,
            f"{ES_INDEX_PREFIX}{date_str}"
        )
        self._extra_clients.append(client)
        return client

    def _split_time_windows(self, start_dt: datetime, end_dt: datetime,
                            window_seconds: int = None) -> list:
        """
        将时间范围拆分为固定秒数的小窗口。

        :param start_dt: 开始时间
        :param end_dt: 结束时间
        :param window_seconds: 窗口大小（秒），默认用配置的 WINDOW_MINUTES * 60
        :return: [(start_dt, end_dt), ...] datetime 对象列表
        """
        if window_seconds is None:
            window_seconds = WINDOW_MINUTES * 60

        windows = []
        current = start_dt
        while current < end_dt:
            window_end = min(current + timedelta(seconds=window_seconds), end_dt)
            windows.append((current, window_end))
            current = window_end

        return windows

    def _build_query_body(self, start_dt: datetime, end_dt: datetime) -> dict:
        """
        构建 ES 查询 body

        :param start_dt: 开始时间（北京时间）datetime
        :param end_dt: 结束时间（北京时间）datetime
        """
        gte_utc = self._to_utc(start_dt)
        lte_utc = self._to_utc(end_dt)

        filters = [
            {"match_all": {}},
            {"term": {"function.keyword": "parseRequestV2"}},
            {
                "range": {
                    "@timestamp": {
                        "gte": gte_utc,
                        "lte": lte_utc,
                        "format": "strict_date_optional_time"
                    }
                }
            }
        ]

        # app_id 非空时才添加过滤
        if self.app_id:
            filters.append({"term": {"app_id.keyword": self.app_id}})

        # 场景过滤：path 非空时添加 term
        if self.path:
            filters.append({"term": {"path.keyword": self.path}})

        # 模型过滤：models 非空时按 api_name 精确匹配
        if self.models:
            filters.append({"terms": {"api_name.keyword": self.models}})

        return {
            "query": {
                "bool": {
                    "must": [],
                    "filter": filters,
                    "should": [],
                    "must_not": []
                }
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc",
                        "unmapped_type": "boolean"
                    }
                }
            ]
        }

    async def _query_window_adaptive(self, win_start: datetime, win_end: datetime,
                                      f, status_callback: Callable,
                                      base_count: int, win_label: str,
                                      scroll_size: int = None) -> int:
        """
        对单个窗口执行查询，遇到 2GB 错误时自动降档重试。

        降档策略：
        - 默认用当前窗口的完整时间范围
        - 2GB 报错 → 拆成 30s 子窗口重试
        - 再报错 → 拆成 15s 子窗口重试
        - 15s 仍报错 → 向上抛出异常

        降档只作用于当前窗口，不影响后续窗口。
        """
        span_seconds = int((win_end - win_start).total_seconds())

        # 尝试直接查询（不拆分）
        body = self._build_query_body(win_start, win_end)
        es = self._get_es_for_date(win_start.strftime("%Y-%m-%d"))
        try:
            count = await es.query_to_file_appender(body, f, status_callback, base_count,
                                                     scroll_size=scroll_size)
            return count
        except Exception as e:
            if not _is_2gb_error(e):
                raise

        logger.warning(f"[adaptive] 2GB hit on {win_label} ({span_seconds}s), trying sub-windows")

        # 依次尝试更小的窗口
        for tier_seconds in _WINDOW_SECONDS_TIERS:
            if tier_seconds >= span_seconds:
                continue  # 跳过比当前窗口还大或相等的档位

            sub_windows = self._split_time_windows(win_start, win_end, tier_seconds)
            logger.info(f"[adaptive] retrying {win_label} with {tier_seconds}s windows ({len(sub_windows)} sub-windows)")

            if status_callback:
                status_callback(base_count, f"{win_label} 降档至 {tier_seconds}s 窗口重试...")

            sub_count = 0
            sub_failed = False
            for sub_idx, (sw_start, sw_end) in enumerate(sub_windows):
                sub_body = self._build_query_body(sw_start, sw_end)
                sub_es = self._get_es_for_date(sw_start.strftime("%Y-%m-%d"))
                try:
                    c = await sub_es.query_to_file_appender(
                        sub_body, f, status_callback, base_count + sub_count,
                        scroll_size=scroll_size
                    )
                    sub_count += c
                except Exception as sub_e:
                    if _is_2gb_error(sub_e):
                        logger.warning(f"[adaptive] 2GB hit on sub-window {sub_idx + 1}/{len(sub_windows)} "
                                       f"({tier_seconds}s), will try next tier")
                        sub_failed = True
                        break
                    raise  # 非 2GB 错误直接抛出

            if not sub_failed:
                logger.info(f"[adaptive] {win_label} OK with {tier_seconds}s windows, count={sub_count}")
                return sub_count

        # 所有档位都失败
        raise RuntimeError(
            f"ES 2GB 限制：{win_label} 即使 {_WINDOW_SECONDS_TIERS[-1]}s 窗口仍超限，"
            f"请检查该时段数据量或联系管理员"
        )

    async def query(self, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """
        查询指定时间范围内的数据（小数据量使用）
        """
        start_dt = self._parse_datetime(start_time)
        end_dt = self._parse_datetime(end_time)
        body = self._build_query_body(start_dt, end_dt)
        return await self.es.query(body)

    async def query_to_dir(self, start_time: str, end_time: str, output_dir: str,
                           status_callback: Callable = None,
                           window_concurrency: int = 10,
                           scroll_size: int = None) -> dict:
        """
        流式查询并按窗口分别写入 output_dir（每个窗口一个 .jsonl 文件）。
        无需 merge 步骤，每个 1 分钟窗口直接写入最终文件。

        :param start_time: 开始时间（北京时间），格式 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :param end_time: 结束时间（北京时间），格式同上
        :param output_dir: 输出目录，按窗口写入 kv_{start}_{end}.jsonl
        :param status_callback: 状态回调函数 (count, message)
        :param window_concurrency: 窗口内并发数（默认 10）
        :return: {"total_count": int, "files": [{"file": path, "minute": int, "count": int}, ...]}
        """
        start_dt = self._parse_datetime(start_time)
        end_dt = self._parse_datetime(end_time)
        windows = self._split_time_windows(start_dt, end_dt)

        n_windows = len(windows)

        # 单窗口：直接写文件
        if n_windows <= 1:
            total_count = 0
            files_info = []
            for win_idx, (win_start, win_end) in enumerate(windows):
                win_start_str = win_start.strftime("%Y%m%d_%H%M%S")
                win_end_str = win_end.strftime("%Y%m%d_%H%M%S")
                final_file = os.path.join(output_dir, f"kv_{win_start_str}_{win_end_str}.jsonl")
                incomplete_file = final_file + ".incomplete"

                win_label = f"窗口 {win_idx + 1}/{n_windows}"
                if status_callback:
                    status_callback(total_count, f"正在查询{win_label}，已获取 {total_count} 条...")

                with open(incomplete_file, 'w', encoding='utf-8', buffering=16 * 1024 * 1024) as f:
                    window_count = await self._query_window_adaptive(
                        win_start, win_end, f, status_callback, total_count, win_label,
                        scroll_size=scroll_size
                    )
                    total_count += window_count

                if os.path.exists(incomplete_file):
                    os.rename(incomplete_file, final_file)
                files_info.append({
                    "file": final_file,
                    "minute": win_start.minute,
                    "count": window_count,
                })

            if status_callback:
                status_callback(total_count, f"查询完成，共 {total_count} 条记录")
            return {"total_count": total_count, "files": files_info}

        # 多窗口：并行 fetch 直写目录（无 merge）
        return await self._query_parallel_to_dir(
            windows, output_dir, status_callback, window_concurrency,
            scroll_size=scroll_size
        )

    async def _query_parallel_to_dir(self, windows: list, output_dir: str,
                                     status_callback: Callable,
                                     window_concurrency: int,
                                     scroll_size: int = None) -> dict:
        """
        多窗口并行查询，每个窗口直接写入 output_dir 中的独立文件。
        遇到 scroll context 超限时自动降低并发重试（最多 3 次）。
        """
        max_retries = 3
        current_concurrency = window_concurrency

        for attempt in range(max_retries + 1):
            try:
                return await self._do_parallel_fetch_to_dir(
                    windows, output_dir, status_callback, current_concurrency,
                    scroll_size=scroll_size
                )
            except Exception as e:
                if _is_scroll_limit_error(e) and attempt < max_retries:
                    old_concurrency = current_concurrency
                    current_concurrency = max(1, current_concurrency // 2)
                    logger.warning(
                        f"[fetch] scroll context 超限 (attempt {attempt + 1}/{max_retries})，"
                        f"并发从 {old_concurrency} 降至 {current_concurrency}，等待 30s 后重试..."
                    )
                    if status_callback:
                        status_callback(0, f"scroll context 超限，并发降至 {current_concurrency}，等待重试...")
                    await asyncio.sleep(30)
                    self._cleanup_incomplete_files(output_dir, windows)
                    continue
                raise

        raise RuntimeError("scroll context 重试次数用尽")

    def _cleanup_incomplete_files(self, output_dir: str, windows: list):
        """清理并行查询产生的 .incomplete 文件"""
        for win_start, win_end in windows:
            s_tag = win_start.strftime("%Y%m%d_%H%M%S")
            e_tag = win_end.strftime("%Y%m%d_%H%M%S")
            incomplete_file = os.path.join(output_dir, f"kv_{s_tag}_{e_tag}.jsonl.incomplete")
            try:
                if os.path.exists(incomplete_file):
                    os.remove(incomplete_file)
            except OSError:
                pass

    async def _do_parallel_fetch_to_dir(self, windows: list, output_dir: str,
                                        status_callback: Callable,
                                        window_concurrency: int,
                                        scroll_size: int = None) -> dict:
        """执行一次并行 fetch，每个窗口直接写入独立 .jsonl 文件（无 merge）"""
        n_windows = len(windows)
        logger.info(f"[fetch] 并行直写模式: {n_windows} 个窗口, 并发={window_concurrency}")
        sem = asyncio.Semaphore(window_concurrency)
        window_results = [None] * n_windows  # (incomplete_file, final_file, count) or None

        _progress_lock = threading.Lock()
        _shared_count = [0]
        _done_windows = [0]

        async def _fetch_window(idx: int, win_start: datetime, win_end: datetime):
            async with sem:
                s_tag = win_start.strftime("%Y%m%d_%H%M%S")
                e_tag = win_end.strftime("%Y%m%d_%H%M%S")
                final_file = os.path.join(output_dir, f"kv_{s_tag}_{e_tag}.jsonl")
                incomplete_file = final_file + ".incomplete"

                win_start_str = win_start.strftime("%Y-%m-%d %H:%M:%S")
                win_end_str = win_end.strftime("%Y-%m-%d %H:%M:%S")
                win_label = f"窗口 {idx + 1}/{n_windows} ({win_start_str}~{win_end_str})"

                def _win_callback(count, msg):
                    if status_callback:
                        with _progress_lock:
                            status_callback(
                                _shared_count[0],
                                f"[{_done_windows[0]}/{n_windows}完成] {win_label}: {msg}"
                            )

                with open(incomplete_file, 'w', encoding='utf-8', buffering=16 * 1024 * 1024) as f:
                    count = await self._query_window_adaptive(
                        win_start, win_end, f, _win_callback, 0, win_label,
                        scroll_size=scroll_size
                    )

                gc.collect()
                _malloc_trim()

                with _progress_lock:
                    _shared_count[0] += count
                    _done_windows[0] += 1

                window_results[idx] = (incomplete_file, final_file, count)
                logger.info(f"[fetch] {win_label} 完成: {count} 条 (累计 {_shared_count[0]})")

        tasks = [
            asyncio.create_task(_fetch_window(i, ws, we))
            for i, (ws, we) in enumerate(windows)
        ]

        try:
            await asyncio.gather(*tasks)
        except Exception:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for wr in window_results:
                if wr is not None:
                    try:
                        if os.path.exists(wr[0]):
                            os.remove(wr[0])
                    except OSError:
                        pass
            raise

        # 重命名 .incomplete → .jsonl（替代 merge）
        total_count = 0
        files_info = []
        for idx in range(n_windows):
            if window_results[idx] is None:
                continue
            incomplete_file, final_file, count = window_results[idx]
            total_count += count
            if os.path.exists(incomplete_file):
                os.rename(incomplete_file, final_file)
            elif count == 0:
                # 零记录窗口：确保空文件存在
                open(final_file, 'w').close()
            win_start = windows[idx][0]
            files_info.append({
                "file": final_file,
                "minute": win_start.minute,
                "count": count,
            })

        logger.info(f"[fetch] 直写完成: {total_count} 条, {len(files_info)} 个文件")
        if status_callback:
            status_callback(total_count, f"查询完成，共 {total_count} 条记录")

        return {"total_count": total_count, "files": files_info}

    # ---- 旧版 query_to_file（保留供非 k8s 模式使用） ----

    async def query_to_file(self, start_time: str, end_time: str, output_file: str,
                            status_callback: Callable = None,
                            window_concurrency: int = 10) -> int:
        """
        流式查询并写入文件（JSONL 格式，大数据量推荐）。
        自动按窗口拆分时间，窗口间并行查询，最终按时间顺序合并。
        遇到 2GB 错误时自适应降档（60s → 30s → 15s → 5s）。
        遇到 scroll context 超限时自动降低并发重试。

        :param start_time: 开始时间（北京时间），格式 HH:MM:SS 或 YYYY-MM-DD HH:MM:SS
        :param end_time: 结束时间（北京时间），格式同上
        :param output_file: 输出文件路径
        :param status_callback: 状态回调函数 (count, message)
        :param window_concurrency: 窗口内并发数（默认 10），控制同时进行的 ES scroll 数
        :return: 总记录数
        """
        start_dt = self._parse_datetime(start_time)
        end_dt = self._parse_datetime(end_time)
        windows = self._split_time_windows(start_dt, end_dt)

        n_windows = len(windows)

        # 单窗口：走简单路径（不创建临时文件）
        if n_windows <= 1:
            total_count = 0
            with open(output_file, 'w', encoding='utf-8', buffering=16 * 1024 * 1024) as f:
                for win_idx, (win_start, win_end) in enumerate(windows):
                    win_start_str = win_start.strftime("%Y-%m-%d %H:%M:%S")
                    win_end_str = win_end.strftime("%Y-%m-%d %H:%M:%S")
                    win_label = f"窗口 {win_idx + 1}/{n_windows} ({win_start_str}~{win_end_str})"
                    if status_callback:
                        status_callback(total_count, f"正在查询{win_label}，已获取 {total_count} 条...")
                    window_count = await self._query_window_adaptive(
                        win_start, win_end, f, status_callback, total_count, win_label
                    )
                    total_count += window_count
            if status_callback:
                status_callback(total_count, f"查询完成，共 {total_count} 条记录")
            return total_count

        # 多窗口：带 scroll limit 自动降级的并行查询
        return await self._query_parallel_windows(
            windows, output_file, status_callback, window_concurrency
        )

    async def _query_parallel_windows(self, windows: list, output_file: str,
                                       status_callback: Callable,
                                       window_concurrency: int) -> int:
        """
        多窗口并行查询，各写临时文件，最后按顺序合并。
        遇到 scroll context 超限时自动降低并发重试（最多 3 次）。
        """
        max_retries = 3
        current_concurrency = window_concurrency

        for attempt in range(max_retries + 1):
            try:
                return await self._do_parallel_fetch(
                    windows, output_file, status_callback, current_concurrency
                )
            except Exception as e:
                if _is_scroll_limit_error(e) and attempt < max_retries:
                    old_concurrency = current_concurrency
                    # 每次降一半，最低到 1
                    current_concurrency = max(1, current_concurrency // 2)
                    logger.warning(
                        f"[fetch] scroll context 超限 (attempt {attempt + 1}/{max_retries})，"
                        f"并发从 {old_concurrency} 降至 {current_concurrency}，等待 30s 后重试..."
                    )
                    if status_callback:
                        status_callback(0, f"scroll context 超限，并发降至 {current_concurrency}，等待重试...")
                    # 等待已有 scroll context 过期释放
                    await asyncio.sleep(30)
                    # 清理可能存在的残留临时文件
                    self._cleanup_temp_files(output_file, len(windows))
                    continue
                raise

        # 不应到这里，但防御性处理
        raise RuntimeError("scroll context 重试次数用尽")

    def _cleanup_temp_files(self, output_file: str, n_windows: int):
        """清理并行查询产生的临时文件"""
        for idx in range(n_windows):
            temp_file = f"{output_file}.win_{idx:04d}.tmp"
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except OSError:
                pass

    async def _do_parallel_fetch(self, windows: list, output_file: str,
                                  status_callback: Callable,
                                  window_concurrency: int) -> int:
        """执行一次并行 fetch 尝试（可能抛出 scroll limit 异常）"""
        n_windows = len(windows)
        logger.info(f"[fetch] 并行模式: {n_windows} 个窗口, 并发={window_concurrency}")
        sem = asyncio.Semaphore(window_concurrency)
        window_results = [None] * n_windows  # (temp_file, count) or None

        # 线程安全的进度计数（scroll 回调来自线程池）
        _progress_lock = threading.Lock()
        _shared_count = [0]
        _done_windows = [0]

        async def _fetch_window(idx: int, win_start: datetime, win_end: datetime):
            async with sem:
                temp_file = f"{output_file}.win_{idx:04d}.tmp"
                win_start_str = win_start.strftime("%Y-%m-%d %H:%M:%S")
                win_end_str = win_end.strftime("%Y-%m-%d %H:%M:%S")
                win_label = f"窗口 {idx + 1}/{n_windows} ({win_start_str}~{win_end_str})"

                def _win_callback(count, msg):
                    if status_callback:
                        with _progress_lock:
                            status_callback(
                                _shared_count[0],
                                f"[{_done_windows[0]}/{n_windows}完成] {win_label}: {msg}"
                            )

                with open(temp_file, 'w', encoding='utf-8', buffering=16 * 1024 * 1024) as f:
                    count = await self._query_window_adaptive(
                        win_start, win_end, f, _win_callback, 0, win_label
                    )

                # 窗口完成后立即回收内存（scroll response 在线程堆上残留）
                gc.collect()
                _malloc_trim()

                with _progress_lock:
                    _shared_count[0] += count
                    _done_windows[0] += 1

                window_results[idx] = (temp_file, count)
                logger.info(f"[fetch] {win_label} 完成: {count} 条 (累计 {_shared_count[0]})")

        # 启动所有窗口任务
        tasks = [
            asyncio.create_task(_fetch_window(i, ws, we))
            for i, (ws, we) in enumerate(windows)
        ]

        try:
            await asyncio.gather(*tasks)
        except Exception:
            # 出错时取消未完成的任务
            for t in tasks:
                if not t.done():
                    t.cancel()
            # 等待所有任务结束（包括取消的）
            await asyncio.gather(*tasks, return_exceptions=True)
            # 清理已创建的临时文件
            for wr in window_results:
                if wr is not None:
                    try:
                        os.remove(wr[0])
                    except OSError:
                        pass
            raise

        # 按窗口顺序合并临时文件 → 最终输出文件
        total_count = 0
        _MERGE_BUF = 64 * 1024 * 1024
        logger.info(f"[fetch] 开始合并 {n_windows} 个窗口临时文件 (buf={_MERGE_BUF // (1024*1024)}MB)...")
        merge_t0 = asyncio.get_event_loop().time()

        def _merge_files():
            nonlocal total_count
            with open(output_file, 'wb', buffering=_MERGE_BUF) as f_out:
                for idx in range(n_windows):
                    if window_results[idx] is None:
                        continue
                    temp_file, count = window_results[idx]
                    total_count += count
                    if count > 0 and os.path.exists(temp_file):
                        with open(temp_file, 'rb', buffering=_MERGE_BUF) as f_in:
                            shutil.copyfileobj(f_in, f_out, length=_MERGE_BUF)

        await asyncio.get_event_loop().run_in_executor(None, _merge_files)

        merge_elapsed = asyncio.get_event_loop().time() - merge_t0
        logger.info(f"[fetch] 合并完成: {total_count} 条, 耗时 {merge_elapsed:.1f}s")

        # 清理临时文件
        for wr in window_results:
            if wr is not None:
                try:
                    os.remove(wr[0])
                except OSError:
                    pass

        if status_callback:
            status_callback(total_count, f"查询完成，共 {total_count} 条记录")

        return total_count

from elasticsearch import Elasticsearch
from datetime import datetime
import asyncio
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional, Callable

# 创建线程池，避免阻塞事件循环
_executor = ThreadPoolExecutor(max_workers=4)

# ES 专用日志（写入 es_logs/ 目录）
logger = logging.getLogger("es_query")


def trim_ms(t: str) -> int:
    """将 '123ms' 格式转换为整数"""
    if t == '' or t is None:
        return 0
    try:
        res = int(str(t).replace('ms', ''))
        if res > 1000000:
            return 0
        return res
    except (ValueError, AttributeError):
        return 0


def parse_single_record(r: Dict) -> Optional[Dict[str, Any]]:
    """解析单条 ES 记录"""
    return r
    # 暂时注释解析逻辑
    # try:
    #     s = r['_source']
    #     as_id = s.get('as_id', '')

    #     # 解析时间
    #     time_str = s.get('time', '')
    #     if time_str:
    #         t = time_str.split("T")
    #         d_str = '%s %s' % (t[0].strip(), t[1].strip()[:-6])
    #         if len(t[1].strip()[:-6]) == 8:
    #             d_str = '%s %s.0' % (t[0].strip(), t[1].strip()[:-6])
    #         d_obj = datetime.strptime(d_str, '%Y-%m-%d %H:%M:%S.%f')
    #         timestamp = d_obj.timestamp()
    #     else:
    #         timestamp = 0

    #     # 解析 @raw 字段
    #     raw = s.get('@raw', '').split(",")
    #     d = {}
    #     for item in raw:
    #         parts = item.split(':')
    #         if len(parts) == 2:
    #             d[parts[0].strip()] = parts[1].strip()

    #     parsed_item = {
    #         'as_id': as_id,
    #         'timestamp': timestamp,
    #         'as_first': trim_ms(d.get('firstTokenElapse', '')),
    #         'cachedTokens': trim_ms(d.get('cachedTokens', '')),
    #         'beforeParseRequestElapse': trim_ms(d.get('beforeParseRequestElapse', '')),
    #         'parseRequestElapse': trim_ms(d.get('parseRequestElapse', '')),
    #         'permissionRuleValidateElapse': trim_ms(d.get('permissionRuleValidateElapse', '')),
    #         'aclCheckElapse': trim_ms(d.get('aclCheckElapse', '')),
    #         'generateConfigElapse': trim_ms(d.get('generateConfigElapse', '')),
    #         'validateParamsElapse': trim_ms(d.get('validateParamsElapse', '')),
    #         'limitServerElapse': trim_ms(d.get('limitServerElapse', '')),
    #         'inputSafetyElapse': trim_ms(d.get('inputSafetyElapse', '')),
    #         'historySafetyElapse': trim_ms(d.get('historySafetyElapse', '')),
    #         'webSearchElapse': trim_ms(d.get('webSearchElapse', '')),
    #         'outputSafetyElapse': trim_ms(d.get('outputSafetyElapse', '')),
    #         'ebLiteFirstTokenElapse': trim_ms(d.get('ebLiteFirstTokenElapse', '')),
    #         'imageReviewElapse': trim_ms(d.get('imageReviewElapse', '0')),
    #         'preProcessMessageElapse': trim_ms(d.get('preProcessMessageElapse', '')),
    #         'totalTokenElapse': trim_ms(d.get('totalTokenElapse', '')),
    #         'promptTokens': d.get('promptTokens', 0),
    #         'completionTokens': d.get('completionTokens', 0),
    #         'afterParseRequestElapse': trim_ms(d.get('afterParseRequestElapse', '')),
    #         'iamMiddleWareElapse': trim_ms(d.get('iamMiddleWareElapse', '')),
    #         'traceMiddleWareElapse': trim_ms(d.get('traceMiddleWareElapse', '')),
    #         'safetyStreamElapse': trim_ms(d.get('safetyStreamElapse', '')),
    #     }
    #     # 计算 as_cost
    #     parsed_item['as_cost'] = parsed_item['as_first'] - parsed_item['ebLiteFirstTokenElapse']
    #     return parsed_item
    # except Exception:
    #     return None


class ESIndexClient:
    def __init__(self, hosts: str, http_auth: tuple, index: str):
        """创建索引连接"""
        self.index = index
        self.client = Elasticsearch(hosts, http_auth=http_auth, timeout=60)

    def _sync_query_to_file(self, body: dict, output_file: str, status_callback: Callable = None) -> int:
        """
        流式查询并直接写入文件，避免内存溢出
        :param body: ES 查询 body
        :param output_file: 输出文件路径
        :param status_callback: 状态回调函数 (processed_count, message)
        :return: 总记录数
        """
        scroll_time = "10m"
        scroll_size = 10000
        total_count = 0
        scroll_id = None
        t0 = time.time()
        scroll_rounds = 0

        logger.info(f"[scroll_to_file] START index={self.index} output={output_file}")

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('[\n')  # JSON 数组开始
                first_record = True

                # 首次查询
                res = self.client.search(index=self.index, body=body, scroll=scroll_time, size=scroll_size)
                scroll_id = res.get("_scroll_id")
                results = res["hits"]["hits"]
                total_hits = res["hits"].get("total", {}).get("value", "?")
                logger.info(f"[scroll_to_file] first_search total_hits={total_hits} scroll_id={scroll_id[:16]}...")

                while len(results) > 0:
                    scroll_rounds += 1
                    for r in results:
                        parsed = parse_single_record(r)
                        if parsed:
                            if not first_record:
                                f.write(',\n')
                            json.dump(parsed, f, ensure_ascii=False)
                            first_record = False
                            total_count += 1

                    if status_callback:
                        status_callback(total_count, f"已处理 {total_count} 条记录...")

                    res = self.client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                    scroll_id = res.get("_scroll_id")
                    results = res['hits']['hits']

                f.write('\n]')  # JSON 数组结束
        except Exception as e:
            logger.error(f"[scroll_to_file] ERROR index={self.index} rounds={scroll_rounds} count={total_count} err={e}")
            raise
        finally:
            elapsed = time.time() - t0
            if scroll_id:
                try:
                    self.client.clear_scroll(scroll_id=scroll_id)
                    logger.info(f"[scroll_to_file] clear_scroll OK")
                except Exception as ce:
                    logger.warning(f"[scroll_to_file] clear_scroll FAILED: {ce}")
            logger.info(f"[scroll_to_file] END index={self.index} count={total_count} rounds={scroll_rounds} elapsed={elapsed:.1f}s")

        return total_count

    def _sync_query(self, body: dict) -> List[Dict]:
        """同步查询方法（保留兼容，小数据量使用）"""
        scroll_time = "10m"
        scroll_size = 10000
        scroll_id = None
        all_results = []
        t0 = time.time()

        logger.info(f"[scroll_query] START index={self.index}")

        try:
            res = self.client.search(index=self.index, body=body, scroll=scroll_time, size=scroll_size)
            scroll_id = res.get("_scroll_id")
            results = res["hits"]["hits"]

            while len(results) > 0:
                for r in results:
                    parsed = parse_single_record(r)
                    if parsed:
                        all_results.append(parsed)
                res = self.client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                scroll_id = res.get("_scroll_id")
                results = res['hits']['hits']
        except Exception as e:
            logger.error(f"[scroll_query] ERROR index={self.index} count={len(all_results)} err={e}")
            raise
        finally:
            elapsed = time.time() - t0
            if scroll_id:
                try:
                    self.client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass
            logger.info(f"[scroll_query] END index={self.index} count={len(all_results)} elapsed={elapsed:.1f}s")

        return all_results

    async def query(self, body: dict) -> List[Dict[str, Any]]:
        """异步查询方法（小数据量使用）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_executor, self._sync_query, body)

    async def query_to_file(self, body: dict, output_file: str, status_callback: Callable = None) -> int:
        """
        异步流式查询并写入文件（大数据量推荐）
        :return: 总记录数
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor,
            self._sync_query_to_file,
            body,
            output_file,
            status_callback
        )

    def _sync_query_to_file_appender(self, body: dict, f, first_record: bool,
                                      status_callback: Callable, base_count: int) -> int:
        """
        流式查询并追加写入到已打开的文件句柄（用于多窗口拼接）
        """
        scroll_time = "10m"
        scroll_size = 10000
        window_count = 0
        scroll_id = None
        t0 = time.time()

        logger.info(f"[scroll_appender] START index={self.index} base_count={base_count}")

        try:
            res = self.client.search(index=self.index, body=body, scroll=scroll_time, size=scroll_size)
            scroll_id = res.get("_scroll_id")
            results = res["hits"]["hits"]

            while len(results) > 0:
                for r in results:
                    parsed = parse_single_record(r)
                    if parsed:
                        if not first_record or window_count > 0:
                            f.write(',\n')
                        json.dump(parsed, f, ensure_ascii=False)
                        window_count += 1

                if status_callback:
                    status_callback(base_count + window_count, f"已处理 {base_count + window_count} 条记录...")

                res = self.client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                scroll_id = res.get("_scroll_id")
                results = res['hits']['hits']
        except Exception as e:
            logger.error(f"[scroll_appender] ERROR index={self.index} window_count={window_count} err={e}")
            raise
        finally:
            elapsed = time.time() - t0
            if scroll_id:
                try:
                    self.client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass
            logger.info(f"[scroll_appender] END index={self.index} window_count={window_count} elapsed={elapsed:.1f}s")

        return window_count

    async def query_to_file_appender(self, body: dict, f, first_record: bool,
                                      status_callback: Callable = None, base_count: int = 0) -> int:
        """
        异步流式查询并追加写入到文件句柄（用于多窗口拼接）
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor,
            self._sync_query_to_file_appender,
            body, f, first_record, status_callback, base_count
        )

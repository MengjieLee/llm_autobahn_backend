import logging
import asyncio
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import sqlparse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlparse.tokens import DML, Comment, String
from async_lru import alru_cache

from app.conf.config import settings


logger = logging.getLogger(__name__)

def has_limit_clause(parsed_sql) -> bool:
    """检测SQL是否包含LIMIT子句（仅检测关键字，不解析数值）"""
    for token in parsed_sql.flatten():
        if token.ttype in (Comment, String):
            continue
        if token.value.upper() == "LIMIT":
            return True
    return False

def parse_limit_value(parsed_sql) -> int:
    """解析已有LIMIT子句的数值，解析失败返回-1"""
    limit_token_found = False
    for token in parsed_sql.flatten():
        # 跳过注释和字符串
        if token.ttype in (Comment, String):
            continue
        
        # 找到LIMIT关键字后，取下一个有效数字
        if limit_token_found:
            # 过滤空白符
            if token.value.strip() == "":
                continue
            # 尝试解析数字
            try:
                return int(token.value.strip())
            except ValueError:
                # 非数字（比如LIMIT ALL/变量等），返回-1
                return -1
        
        # 标记找到LIMIT关键字
        if token.value.upper() == "LIMIT":
            limit_token_found = True
    return -1

def replace_limit_clause(sql_str: str, new_limit: int) -> str:
    """替换SQL中已有的LIMIT数值为new_limit"""
    parsed = sqlparse.parse(sql_str)[0]
    limit_token_found = False
    new_tokens = []
    
    for token in parsed.flatten():
        # 跳过注释和字符串
        if token.ttype in (Comment, String):
            new_tokens.append(token.value)
            continue
        
        # 找到LIMIT关键字后，替换下一个数值
        if limit_token_found:
            if token.value.strip() == "":
                new_tokens.append(token.value)
                continue
            # 尝试解析原数值，替换为新数值
            try:
                int(token.value.strip())
                new_tokens.append(str(new_limit))
                limit_token_found = False
            except ValueError:
                # 非数字则保留原值
                new_tokens.append(token.value)
                limit_token_found = False
            continue
        
        # 标记找到LIMIT关键字
        if token.value.upper() == "LIMIT":
            new_tokens.append(token.value)
            limit_token_found = True
        else:
            new_tokens.append(token.value)
    
    # 拼接回SQL字符串并清理多余空格
    new_sql = "".join(new_tokens).strip()
    # 确保结尾有分号（保持原格式）
    if not new_sql.endswith(';'):
        new_sql += ';'
    return new_sql

def add_limit_safe(
    sql_input: str,
    limit: int = 1000,
    allow_multi_stmt: bool = False,
    max_limit: int = 1000,
) -> str:
    logger.debug(f"add_limit_safe 输入: {sql_input} | {limit} | {max_limit}")
    # 确保limit不超过max_limit，且最小为1
    target_limit = min(int(limit), max_limit)
    if target_limit < 1:
        target_limit = 1

    parsed_stmts = sqlparse.parse(sql_input)
    if len(parsed_stmts) > 1 and not allow_multi_stmt:
        raise ValueError("不允许执行多语句SQL")

    new_stmts = []
    for stmt in parsed_stmts:
        stmt_str = str(stmt).strip()
        if not stmt_str:
            continue

        # 检测是否有LIMIT子句
        if has_limit_clause(stmt):
            # 解析已有LIMIT的数值
            current_limit = parse_limit_value(stmt)
            if current_limit > target_limit:
                # 已有LIMIT超限，替换为target_limit
                new_stmt = replace_limit_clause(stmt_str, target_limit)
            else:
                # 已有LIMIT未超限，保留原语句
                new_stmt = stmt_str
            new_stmts.append(new_stmt)
            continue

        # 检测SQL类型（SELECT/UPDATE/DELETE）
        stmt_type = None
        for token in stmt.flatten():
            if token.ttype == DML:
                stmt_type = token.value.upper()
                break

        if stmt_type not in ('SELECT', 'UPDATE', 'DELETE'):
            new_stmts.append(stmt_str)
            continue
        
        # 无LIMIT子句，添加限制
        stmt_clean = stmt_str.rstrip(';')
        new_stmt = f"{stmt_clean} LIMIT {target_limit};"
        new_stmts.append(new_stmt)

    return "\n".join(new_stmts)

# --------------------------- 连接器类（核心修改） ---------------------------
class DorisConnectorPydoris:
    """Doris 连接器（适配FastAPI异步场景）。"""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        catalog: str,
        database: str,
    ):
        self.host = host
        self.port = port
        self.user = quote_plus(user)
        self.password = quote_plus(password)
        self.catalog = catalog
        self.database = database
        self.engine: Optional[Engine] = None
        self.connection_string = (
            f"doris://{self.user}:{self.password}@{host}:{port}/{catalog}.{database}"
        )
        logger.info(
            "初始化 Doris 连接 | host=%s port=%s catalog=%s db=%s",
            host,
            port,
            catalog,
            database,
        )

    def _create_engine_sync(self) -> Engine:
        """同步创建引擎（供线程池调用）"""
        if self.engine:
            return self.engine

        try:
            connect_args = {"charset": "utf8mb4"}
            engine = create_engine(
                self.connection_string,
                connect_args=connect_args,
                pool_size=10,          
                max_overflow=20,       
                pool_recycle=3600,     
                pool_pre_ping=True     
            )
            self.engine = engine
            logger.info("已创建 Doris 引擎: %s:%s", self.host, self.port)
            return engine
        except Exception as exc:
            logger.exception("创建 Doris 引擎失败")
            raise

    async def _ensure_engine_async(self) -> Engine:
        """异步创建引擎（将同步操作放到线程池）"""
        if self.engine:
            return self.engine
        # 核心修改：把引擎创建放到线程池
        return await asyncio.to_thread(self._create_engine_sync)

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        try:
            if hasattr(row, "_asdict"):
                return row._asdict()
            return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        except Exception:
            return {k: v for k, v in zip(row.keys(), row)}

    def _sync_test_connection(self, engine: Engine) -> Dict[str, Any]:
        """同步测试连接"""
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1 as test")).fetchone()
            version = connection.execute(text("SELECT version() as version")).fetchone()
            return {
                "ping": self._row_to_dict(result),
                "version": self._row_to_dict(version),
            }

    async def test_connection(self) -> Dict[str, Any]:
        """异步测试连接（完整异步化）"""
        try:
            # 核心修改：先异步创建引擎
            engine = await self._ensure_engine_async()
            # 再异步执行测试
            result = await asyncio.to_thread(self._sync_test_connection, engine)
            return {"code": 0, "message": "ok", "data": result}
        except Exception as exc:
            logger.exception("Doris 连接测试失败")
            return {"code": 59001, "message": f"连接测试失败: {exc}", "data": None}

    def _sync_execute_sql(self, engine: Engine, sql: text) -> List[Dict[str, Any]]:
        """同步执行SQL"""
        res = {"code": 0, "message": "ok", "data": None}
        with engine.connect() as connection:
            if sql.text.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "ALTER")):
                try:
                    result_raw = connection.execute(sql)
                    connection.commit()
                    res["message"] = result_raw.rowcount
                    return res
                except Exception as exc:
                    logger.exception(f"Doris 执行写入失败 | {str(exc)}")
                    res["code"] = 59402
                    res["message"] = str(exc)
                    return res
            else:
                try:
                    result_raw = connection.execute(sql)
                    res["data"] = [self._row_to_dict(row) for row in result_raw.fetchall()]
                    return res
                except Exception as exc:
                    logger.exception(f"Doris 执行查询失败 | {str(exc)}")
                    res["code"] = 59403
                    res["message"] = str(exc)
                    return res

    async def show_table_columns(self, table: str) -> Dict[str, Any]:
        """异步查询表结构"""
        engine = await self._ensure_engine_async()
        full_table_name = f"{self.catalog}.{self.database}.{table.replace('`', '')}"
        sql = text(f"SHOW COLUMNS FROM `{full_table_name}`")
        
        logger.debug("查询表结构 SQL: %s", sql)
        return await asyncio.to_thread(self._sync_execute_sql, engine, sql)

    async def execute_custom_sql(
        self, sql: str, *, limit: int = 1000, allow_multi_stmt: bool = False
    ) -> Dict[str, Any]:

        engine = await self._ensure_engine_async()
        safe_sql = add_limit_safe(
            sql, limit=limit, allow_multi_stmt=allow_multi_stmt
        )
        logger.info("执行 safe SQL: %s", safe_sql)
        
        return await asyncio.to_thread(
            self._sync_execute_sql, engine, text(safe_sql)
        )

    async def show_databases(self) -> Dict[str, Any]:
        return await self.execute_custom_sql("SHOW DATABASES")

    async def show_catalogs(self) -> Dict[str, Any]:
        return await self.execute_custom_sql("SHOW CATALOGS")

    def close(self) -> None:
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Doris 引擎已关闭")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()

# --------------------------- 依赖注入（保留） ---------------------------
async def _build_default_connector() -> DorisConnectorPydoris:
    if not settings.doris_configured:
        raise RuntimeError(
            "Doris 未配置：请设置 DEFAULT_DORIS_HOST/DEFAULT_DORIS_PORT/"
            "DEFAULT_DORIS_USER/DEFAULT_DORIS_CATALOG/DEFAULT_DORIS_DATABASE"
        )
    return DorisConnectorPydoris(
        host=settings.DEFAULT_DORIS_HOST,
        port=settings.DEFAULT_DORIS_PORT,
        user=settings.DEFAULT_DORIS_USER,
        password=settings.DEFAULT_DORIS_PASSWORD,
        catalog=settings.DEFAULT_DORIS_CATALOG,
        database=settings.DEFAULT_DORIS_DATABASE,
    )

@alru_cache(maxsize=1)
async def get_doris_connector() -> DorisConnectorPydoris:
    return await _build_default_connector()

async def close_doris_connector() -> None:
    connector = await get_doris_connector()
    connector.close()
    get_doris_connector.cache_clear()
    logger.info("Doris 连接器已清理")

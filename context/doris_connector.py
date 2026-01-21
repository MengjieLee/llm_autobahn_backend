import logging
import asyncio
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import sqlparse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlparse.tokens import DML
from async_lru import alru_cache

from app.conf.config import settings


logger = logging.getLogger(__name__)

# --------------------------- 工具函数（保留不变） ---------------------------
def has_limit_clause(parsed_sql) -> bool:
    for token in parsed_sql.flatten():
        if token.ttype in (sqlparse.tokens.Comment, sqlparse.tokens.String):
            continue
        if token.value.upper() == "LIMIT":
            return True
    return False

def add_limit_safe(
    sql_input: str,
    limit: int = 1000,
    allow_multi_stmt: bool = False,
    max_limit: int = 1000,
) -> str:
    limit = min(int(limit), max_limit)
    if limit < 1:
        limit = 1

    parsed_stmts = sqlparse.parse(sql_input)
    if len(parsed_stmts) > 1 and not allow_multi_stmt:
        raise ValueError("不允许执行多语句SQL")

    new_stmts = []
    for stmt in parsed_stmts:
        stmt_str = str(stmt).strip()
        if not stmt_str:
            continue

        if has_limit_clause(stmt):
            new_stmts.append(stmt_str)
            continue

        stmt_type = None
        for token in stmt.flatten():
            if token.ttype == DML:
                stmt_type = token.value.upper()
                break

        if stmt_type == "SELECT":
            stmt_clean = stmt_str.rstrip(";")
            new_stmt = f"{stmt_clean} LIMIT {limit};"
            new_stmts.append(new_stmt)
        else:
            new_stmts.append(stmt_str)

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
            return {"errcode": 0, "message": "ok", "data": result}
        except Exception as exc:
            logger.exception("Doris 连接测试失败")
            return {"errcode": 59001, "message": f"连接测试失败: {exc}", "data": None}

    def _sync_execute_sql(self, engine: Engine, sql: text) -> List[Dict[str, Any]]:
        """同步执行SQL"""
        with engine.connect() as connection:
            if sql.text.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "ALTER")):
                result_raw = connection.execute(sql)
                connection.commit()
                return [{"affected_rows": result_raw.rowcount}]
            else:
                result_raw = connection.execute(sql)
                return [self._row_to_dict(row) for row in result_raw.fetchall()]

    async def show_table_columns(self, table: str) -> Dict[str, Any]:
        """异步查询表结构"""
        try:
            engine = await self._ensure_engine_async()
            full_table_name = f"{self.catalog}.{self.database}.{table.replace('`', '')}"
            sql = text(f"SHOW COLUMNS FROM `{full_table_name}`")
            
            logger.debug("查询表结构 SQL: %s", sql)
            columns = await asyncio.to_thread(self._sync_execute_sql, engine, sql)
            return {"errcode": 0, "message": "ok", "data": columns}
        except Exception as exc:
            logger.exception("查询表列信息失败: table=%s", table)
            return {"errcode": 59401, "message": f"查询表结构失败: {exc}", "data": []}

    async def execute_custom_sql(
        self, sql: str, *, limit: int = 1000, allow_multi_stmt: bool = False
    ) -> Dict[str, Any]:
        """异步执行自定义SQL"""
        result: Dict[str, Any] = {"errcode": 0, "message": "请求成功", "data": []}

        try:
            engine = await self._ensure_engine_async()
            safe_sql = add_limit_safe(
                sql, limit=limit, allow_multi_stmt=allow_multi_stmt
            )
            logger.info("执行 safe SQL: %s", safe_sql)
            
            records = await asyncio.to_thread(
                self._sync_execute_sql, engine, text(safe_sql)
            )

            if safe_sql.strip().upper().startswith(("SELECT", "SHOW", "DESCRIBE", "DESC")):
                result["data"] = records
                if not records:
                    result["errcode"] = 59200
                    result["message"] = "查询无结果"
            else:
                result["message"] = "非查询语句执行成功"

        except SQLAlchemyError as exc:
            logger.exception("执行 SQLAlchemy 失败")
            result["errcode"] = 59400
            result["message"] = f"执行SQL失败: {exc}"
        except ValueError as exc:
            logger.warning("SQL 校验失败: %s", exc)
            result["errcode"] = 59101
            result["message"] = str(exc)
        except Exception as exc:
            logger.exception("执行 SQL 发生未知错误")
            result["errcode"] = 59500
            result["message"] = f"执行SQL发生未知错误: {exc}"

        return result

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

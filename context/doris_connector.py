import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import sqlparse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlparse.tokens import DML

from app.conf.config import settings


logger = logging.getLogger(__name__)


def has_limit_clause(parsed_sql) -> bool:
    """语法级检测SQL是否包含LIMIT子句（排除注释/字符串中的LIMIT）"""
    for token in parsed_sql.flatten():
        if token.ttype in (sqlparse.tokens.Comment,):
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
    """
    生产级：安全添加LIMIT（防注入+边界处理）
    :param sql_input: 原始SQL
    :param limit: 默认LIMIT值（不超过max_limit）
    :param allow_multi_stmt: 是否允许多语句（默认禁止）
    :param max_limit: 最大允许的LIMIT值（防恶意大数值）
    :return: 安全的SQL语句
    """
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

        if stmt_type not in ("SELECT", "UPDATE", "DELETE"):
            new_stmts.append(stmt_str)
            continue

        stmt_clean = stmt_str.rstrip(";")
        new_stmt = f"{stmt_clean} LIMIT {limit};"
        new_stmts.append(new_stmt)

    return "\n".join(new_stmts)


class DorisConnectorPydoris:
    """Doris 连接器（无 Streamlit 依赖，使用日志与异常）。"""

    @staticmethod
    def _row_to_dict(row):
        return row._asdict() if hasattr(row, "_asdict") else dict(row)

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
        self.user = user
        self.password = password
        self.catalog = catalog
        self.database = database
        self.engine: Optional[Engine] = None

        encoded_user = quote_plus(user)
        encoded_password = quote_plus(password)
        self.connection_string = (
            f"doris://{encoded_user}:{encoded_password}"
            f"@{host}:{port}/{catalog}.{database}"
        )
        logger.info(
            "初始化 Doris 连接 | host=%s port=%s catalog=%s db=%s",
            host,
            port,
            catalog,
            database,
        )

    def _ensure_engine(self) -> Engine:
        if self.engine:
            return self.engine

        try:
            connect_args = {"charset": "utf8mb4"}
            self.engine = create_engine(
                self.connection_string, connect_args=connect_args
            )
            logger.info("已创建 Doris 引擎: %s:%s", self.host, self.port)
            return self.engine
        except Exception as exc:  # noqa: BLE001
            logger.exception("创建 Doris 引擎失败")
            raise

    def test_connection(self) -> Dict[str, Any]:
        try:
            engine = self._ensure_engine()
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1 as test")).fetchone()
                version = connection.execute(
                    text("SELECT version() as version")
                ).fetchone()
                payload = {
                    "ping": self._row_to_dict(result),
                    "version": self._row_to_dict(version),
                }
                logger.info("Doris 连接测试成功: %s", payload)
                return {"errcode": 0, "message": "ok", "data": payload}
        except Exception as exc:  # noqa: BLE001
            logger.exception("Doris 连接测试失败")
            return {"errcode": 59001, "message": f"连接测试失败: {exc}", "data": None}

    def show_table_columns(self, table: str) -> Dict[str, Any]:
        try:
            engine = self._ensure_engine()
            full_table_name = f"{self.catalog}.{self.database}.{table}"
            sql = f"SHOW COLUMNS FROM {full_table_name}"
            logger.debug("查询表结构 SQL: %s", sql)
            with engine.connect() as connection:
                result = connection.execute(text(sql))
                columns = [self._row_to_dict(row) for row in result.fetchall()]
                return {"errcode": 0, "message": "ok", "data": columns}
        except Exception as exc:  # noqa: BLE001
            logger.exception("查询表列信息失败: table=%s", table)
            return {"errcode": 59401, "message": f"查询表结构失败: {exc}", "data": []}

    def execute_custom_sql(
        self, sql: str, *, limit: int = 1000, allow_multi_stmt: bool = False
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"errcode": 0, "message": "请求成功", "data": []}

        try:
            engine = self._ensure_engine()
            safe_sql = add_limit_safe(
                sql, limit=limit, allow_multi_stmt=allow_multi_stmt
            )
            logger.debug("执行 SQL: %s", safe_sql)
            with engine.connect() as connection:
                result_raw = connection.execute(text(safe_sql))

                if safe_sql.strip().upper().startswith(
                    ("SELECT", "SHOW", "DESCRIBE", "DESC")
                ):
                    records = [self._row_to_dict(row) for row in result_raw.fetchall()]
                    result["data"] = records
                    if not records:
                        result["errcode"] = 59200
                        result["message"] = "查询无结果"
                else:
                    result["errcode"] = 59300
                    result["message"] = "非查询语句已执行"
        except SQLAlchemyError as exc:
            logger.exception("执行 SQLAlchemy 失败")
            result["errcode"] = 59400
            result["message"] = f"执行SQL失败: {exc}"
        except ValueError as exc:
            logger.warning("SQL 校验失败: %s", exc)
            result["errcode"] = 59101
            result["message"] = str(exc)
        except Exception as exc:  # noqa: BLE001
            logger.exception("执行 SQL 发生未知错误")
            result["errcode"] = 59500
            result["message"] = f"执行SQL发生未知错误: {exc}"

        return result

    def show_databases(self) -> Dict[str, Any]:
        return self.execute_custom_sql("SHOW DATABASES")

    def show_catalogs(self) -> Dict[str, Any]:
        return self.execute_custom_sql("SHOW CATALOGS")

    def close(self) -> None:
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Doris 引擎已关闭")


def _build_default_connector() -> DorisConnectorPydoris:
    """
    基于全局配置创建 Doris 连接实例，供 FastAPI 依赖或服务层复用。
    使用 lru_cache 包装的 get_doris_connector() 保证单例。
    """
    return DorisConnectorPydoris(
        host=settings.DEFAULT_DORIS_HOST,
        port=settings.DEFAULT_DORIS_PORT,
        user=settings.DEFAULT_DORIS_USER,
        password=settings.DEFAULT_DORIS_PASSWORD,
        catalog=settings.DEFAULT_DORIS_CATALOG,
        database=settings.DEFAULT_DORIS_DATABASE,
    )


@lru_cache(maxsize=1)
def get_doris_connector() -> DorisConnectorPydoris:
    """
    获取 Doris 连接单例。适合在 FastAPI 依赖注入中使用。
    """
    return _build_default_connector()


def close_doris_connector() -> None:
    """
    主动关闭连接池并清理单例缓存，供应用关闭时调用。
    """
    connector = get_doris_connector()
    connector.close()
    get_doris_connector.cache_clear()

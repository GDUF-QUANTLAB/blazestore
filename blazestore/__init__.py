"""BlazeStore - 本地 Parquet 存储管理器

提供本地 Parquet 文件存储、读取和 SQL 查询功能。

Examples:
    >>> from blazestore import LocalStore, get_settings
    >>> store = LocalStore()
    >>> store.put(df, "stocks")
    >>> store.put(df, "stocks/2024.parquet")
    >>> store.put(df, "stocks", partitions=["date"])
    >>> df = store.read("stocks").collect()
"""

from .api import (
    check_table,
    copy_table,
    delete_table,
    get_actual_mtime,
    get_table_info,
    has,
    list_tables,
    optimize_table,
    put,
    read,
    rename_table,
    sql,
    tb_path,
)
from .config import get_settings
from .local import LocalStore

__all__ = [
    "LocalStore",
    "check_table",
    "copy_table",
    "delete_table",
    "get_actual_mtime",
    "get_settings",
    "get_table_info",
    "has",
    "list_tables",
    "optimize_table",
    "put",
    "read",
    "rename_table",
    "sql",
    "tb_path",
]

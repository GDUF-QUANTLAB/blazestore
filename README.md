# BlazeStore

BlazeStore is a lightweight local Parquet storage manager for Polars workflows.
It provides a small API for writing, reading, listing, querying, and managing
Parquet-backed tables on a local filesystem.

## Install

Install directly from GitHub:

```bash
uv add git+https://github.com/GDUF-QUANTLAB/blazestore.git
```

or with pip:

```bash
pip install git+https://github.com/GDUF-QUANTLAB/blazestore.git
```

## Basic Usage

```python
import polars as pl
from blazestore import LocalStore

store = LocalStore("/tmp/blazestore")

df = pl.DataFrame(
    {
        "date": ["2024-01-01", "2024-01-02"],
        "symbol": ["AAPL", "MSFT"],
        "price": [185.0, 410.0],
    }
)

store.put(df, "prices")
result = store.read("prices").collect()
```

You can also write a single Parquet file:

```python
store.put(df, "prices.parquet")
result = store.read("prices.parquet").collect()
```

Hive-style partitioned tables are supported:

```python
store.put(df, "prices_by_date", partitions=["date"])
result = store.read("prices_by_date").collect()
```

## Storage Objects

BlazeStore manages objects under a single local store directory. There are three
supported object shapes:

- Directory tables, written as `name/data.parquet`.
- Single Parquet files, written as `name.parquet`.
- Hive-style partitioned tables, written as `name/column=value/*.parquet`.

Paths are always relative to the store directory. Empty paths, absolute paths,
and paths containing `..` are rejected so table operations stay inside the store.
Writing to an existing object overwrites it.

## Convenience API

BlazeStore also exposes module-level helpers backed by the configured default
store path:

```python
from blazestore import put, read, sql, list_tables

put(df, "prices")
tables = list_tables()
result = sql("SELECT * FROM prices", lazy=False)
```

The default store path is read from `~/.blaze/config.toml`. If the file does not
exist, BlazeStore creates it with a default path of `~/BlazeStore`.

`sql()` is intended for simple local queries over stored tables. For complex
queries or advanced transformations, prefer `read(...).collect()` or Polars
lazy APIs directly.

## API Overview

- `LocalStore(base_path=None)`: manage a local store explicitly.
- `put(df, path, partitions=None)`: write a DataFrame or LazyFrame.
- `read(path)`: read a stored object as a `polars.LazyFrame`.
- `sql(query, lazy=True)`: run simple SQL against stored tables.
- `list_tables()`, `has(path)`, `tb_path(path="")`: inspect store contents.
- `get_table_info(path)`, `check_table(path)`, `get_actual_mtime(path)`: inspect
  table metadata and health.
- `copy_table(src, dst)`, `rename_table(old, new)`, `delete_table(path)`,
  `optimize_table(path)`: manage stored objects.

## Development

```bash
git clone https://github.com/GDUF-QUANTLAB/blazestore.git
cd blazestore
uv sync
uv run ruff check .
uv run pytest
uv build
```

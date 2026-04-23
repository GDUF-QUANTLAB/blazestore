# BlazeStore

BlazeStore provides local Parquet storage helpers and lightweight database
clients for Polars workflows.

## Development

Create the environment and install dependencies with uv:

```bash
uv sync
```

Run lint checks:

```bash
uv run ruff check .
```

Build the package:

```bash
uv build
```

The `clickhouse_df` dependency is installed directly from:

```text
https://github.com/GDUF-QUANTLAB/clickhouse_df
```

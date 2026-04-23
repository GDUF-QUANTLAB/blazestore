from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from blazestore import (
    LocalStore,
    api,
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
from blazestore.exceptions import PathError


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
            "symbol": ["AAPL", "MSFT", "NVDA"],
            "price": [185.0, 410.0, 900.0],
        }
    )


def assert_frame_equal(left: pl.DataFrame, right: pl.DataFrame) -> None:
    assert left.sort(left.columns).equals(right.sort(right.columns))


def test_put_read_and_list_simple_table(
    tmp_path: Path,
    sample_df: pl.DataFrame,
) -> None:
    store = LocalStore(tmp_path)

    store.put(sample_df, "prices")

    assert store.has("prices")
    assert store.list_tables() == ["prices"]
    assert (tmp_path / "prices" / "data.parquet").exists()
    assert_frame_equal(store.read("prices").collect(), sample_df)


def test_paths_must_stay_inside_store(
    tmp_path: Path,
    sample_df: pl.DataFrame,
) -> None:
    store = LocalStore(tmp_path)

    invalid_paths = ["", ".", "../outside", "nested/../outside", "/tmp/outside"]

    for path in invalid_paths:
        with pytest.raises(PathError):
            store.put(sample_df, path)


def test_put_and_read_single_parquet_file(
    tmp_path: Path,
    sample_df: pl.DataFrame,
) -> None:
    store = LocalStore(tmp_path)

    store.put(sample_df, "prices.parquet")

    assert (tmp_path / "prices.parquet").exists()
    assert store.list_tables() == ["prices.parquet"]
    assert_frame_equal(store.read("prices.parquet").collect(), sample_df)

    info = store.get_table_info("prices.parquet")
    assert info["name"] == "prices.parquet"
    assert info["type"] == "file"
    assert info["rows"] == 3
    assert info["partitions"] is None

    assert store.check_table("prices.parquet")
    assert store.get_actual_mtime("prices.parquet")

    store.copy_table("prices.parquet", "prices_copy.parquet")
    assert_frame_equal(store.read("prices_copy.parquet").collect(), sample_df)

    store.rename_table("prices_copy.parquet", "prices_archive.parquet")
    assert store.has("prices_archive.parquet")

    store.delete_table("prices_archive.parquet")
    assert not store.has("prices_archive.parquet")


def test_put_and_read_partitioned_table(
    tmp_path: Path,
    sample_df: pl.DataFrame,
) -> None:
    store = LocalStore(tmp_path)

    store.put(sample_df, "prices_by_date", partitions=["date"])

    assert store._is_partitioned_table("prices_by_date")
    assert store._get_partition_columns("prices_by_date") == ["date"]
    assert_frame_equal(store.read("prices_by_date").collect(), sample_df)


def test_table_management_operations(
    tmp_path: Path,
    sample_df: pl.DataFrame,
) -> None:
    store = LocalStore(tmp_path)
    store.put(sample_df, "prices")
    store.put(sample_df, "prices.parquet")

    info = store.get_table_info("prices")
    assert info["name"] == "prices"
    assert info["type"] == "simple"
    assert info["rows"] == 3
    assert info["partitions"] is None
    assert store.list_tables() == ["prices", "prices.parquet"]

    assert store.check_table("prices")
    assert store.get_actual_mtime("prices")

    store.copy_table("prices", "prices_copy")
    assert store.has("prices_copy")

    store.rename_table("prices_copy", "prices_archive")
    assert not store.has("prices_copy")
    assert store.has("prices_archive")

    store.optimize_table("prices")
    assert store.check_table("prices")

    store.delete_table("prices_archive")
    assert not store.has("prices_archive")


def test_read_missing_path_raises_path_error(tmp_path: Path) -> None:
    store = LocalStore(tmp_path)

    with pytest.raises(PathError):
        store.read("missing")


def test_module_level_api_uses_configured_store(
    tmp_path: Path,
    sample_df: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api, "_store", LocalStore(tmp_path))

    put(sample_df, "prices")

    assert has("prices")
    assert list_tables() == ["prices"]
    assert tb_path("prices") == tmp_path / "prices"
    assert_frame_equal(read("prices").collect(), sample_df)
    result = sql("SELECT symbol, price FROM prices WHERE price > 400", lazy=False)
    assert result.height == 2

    info = get_table_info("prices")
    assert info["rows"] == 3

    copy_table("prices", "prices_copy")
    rename_table("prices_copy", "prices_archive")
    assert check_table("prices_archive")
    assert get_actual_mtime("prices_archive")
    optimize_table("prices_archive")
    delete_table("prices_archive")
    assert not has("prices_archive")

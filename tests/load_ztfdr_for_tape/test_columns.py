from pathlib import Path
from typing import List

import polars as pl

from load_ztfdr_for_tape import columns


def get_empty_df(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(path, n_rows=0)


def get_column_names(path: Path) -> List[str]:
    return get_empty_df(path).columns


def test_id_column(lc_dr19_single_file):
    assert columns.ID_COLUMN in get_column_names(lc_dr19_single_file)


def test_object_columns(lc_dr19_single_file):
    all_columns = get_column_names(lc_dr19_single_file)
    assert set(all_columns).issuperset(columns.OBJECT_COLUMNS), \
        f"Missing columns: {set(columns.OBJECT_COLUMNS) - set(all_columns)}"


def test_source_columns(lc_dr19_single_file):
    all_columns = get_column_names(lc_dr19_single_file)
    assert set(all_columns).issuperset(columns.SOURCE_COLUMNS), \
        f"Missing columns: {set(columns.SOURCE_COLUMNS) - set(all_columns)}"


def test_all_columns(lc_dr19_single_file):
    desired = get_column_names(lc_dr19_single_file)
    actual = (columns.ID_COLUMN,) + columns.OBJECT_COLUMNS + columns.SOURCE_COLUMNS + columns.UNUSED_COLUMNS
    assert set(desired) == set(actual)

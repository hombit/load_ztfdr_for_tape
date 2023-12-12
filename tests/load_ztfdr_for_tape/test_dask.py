from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from numpy.testing import assert_array_equal

from load_ztfdr_for_tape import columns
from load_ztfdr_for_tape.dask import (derive_dd_divisions, load_object_frame,
                                      load_object_source_frames_from_path,
                                      load_source_frame)
from load_ztfdr_for_tape.filepath import get_ordered_paths


def count_rows(path: Path) -> int:
    paths = path.glob('**/*.parquet')
    return sum(pq.read_metadata(p).num_rows for p in paths)


def count_items_single_file(path: Path, column: str) -> int:
    count = cast(int, pl.read_parquet(path, columns=[column])[column].list.len().sum())
    return count


def count_items(path: Path, column: str) -> int:
    paths = path.glob('**/*.parquet')
    count = sum(count_items_single_file(p, column) for p in paths)
    return count


def test_derive_dd_divisions(lc_dr19):
    ordered_paths = get_ordered_paths(lc_dr19)
    divisions = derive_dd_divisions(ordered_paths)
    assert len(divisions) == len(ordered_paths) + 1
    assert np.all(np.diff(divisions) > 0)


def test_load_object_frame(lc_dr19):
    df = load_object_frame(lc_dr19)

    # Check divisions
    assert all(division is not None for division in df.divisions), "Missing divisions"
    assert df.npartitions == len(df.divisions) - 1, "Wrong number of partitions"
    assert np.all(np.diff(df.divisions) > 0)

    computed = df.compute()

    # Check size of the dataframe
    assert df.shape[0].compute() == computed.shape[0] == count_rows(lc_dr19)

    # Check index
    assert computed.index.name == columns.ID_COLUMN
    assert computed.index.is_unique

    # Check columns
    assert set(computed.columns) == set(columns.OBJECT_COLUMNS)


def test_load_source_frame(lc_dr19):
    df = load_source_frame(lc_dr19)

    # Check divisions
    assert all(division is not None for division in df.divisions), "Missing divisions"
    assert df.npartitions == len(df.divisions) - 1, "Wrong number of partitions"
    assert np.all(np.diff(df.divisions) > 0)

    computed = df.compute()

    # Check size of the dataframe
    assert df.shape[0].compute() == computed.shape[0] == count_items(lc_dr19, columns.TIME_DOMAIN_COLUMNS[0])

    # Check index
    assert computed.index.name == columns.ID_COLUMN
    assert not computed.index.is_unique

    # Check columns
    assert set(computed.columns) == set(columns.SOURCE_COLUMNS)


def test_load_object_source_frames_from_path(lc_dr19):
    objects, sources = load_object_source_frames_from_path(lc_dr19)

    # Check divisions
    assert all(division is not None for division in objects.divisions), "Missing divisions"
    assert objects.divisions == sources.divisions

    objects_computed = objects.compute()
    sources_computed = sources.compute()

    # Check index matches
    assert_array_equal(np.unique(objects_computed.index), np.unique(sources_computed.index))

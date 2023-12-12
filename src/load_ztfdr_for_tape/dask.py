"""Functions for loading ZTF DR data into Dask dataframes."""

from pathlib import Path
from typing import Callable, Iterable, Optional, Tuple, Union, cast

import dask.dataframe as dd
import pandas as pd
import polars as pl

from load_ztfdr_for_tape.columns import (ID_COLUMN, OBJECT_COLUMNS,
                                         SOURCE_COLUMNS)
from load_ztfdr_for_tape.filepath import ParsedDataFilePath, order_paths_by_oid

__all__ = ["load_object_frame", "load_source_frame"]


PathType = Union[str, Path]


def load_object_df(path: PathType, columns: Iterable[str] = OBJECT_COLUMNS) -> pd.DataFrame:
    """Load the "object" dataframe from a ZTF DR datafile.

    It loads all the columns but those that represent light curves.

    Parameters
    ----------
    path : str or Path
        Path to the datafile to load.
    columns : iterable of str
        Columns to load from the datafile. By default, it loads all the
        columns but those that represent light curves.

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the object table.
    """
    polars_df = pl.read_parquet(path, columns=['ID_COLUMN'] + list(columns))
    pandas_df = polars_df.to_pandas(use_pyarrow_extension_array=True)
    pandas_df.set_index(ID_COLUMN, inplace=True)
    return pandas_df


def load_source_df(path: PathType, columns: Iterable[str] = SOURCE_COLUMNS) -> pd.DataFrame:
    """Load the "source" dataframe from a ZTF DR datafile.

    It loads objectid column and columns representing light curves.
    It flattens the nested light curve columns into a single level.

    Parameters
    ----------
    path : str or Path
        Path to the datafile to load.
    columns : iterable of str
        Columns to load from the datafile. By default, it loads objectid
        column and columns representing light curves.

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the source table.
    """
    columns = list(columns)

    polars_nested_df = pl.read_parquet(path, columns=['ID_COLUMN'] + columns)
    polars_flat_df = polars_nested_df.explode(columns)
    pandas_df = polars_flat_df.to_pandas(use_pyarrow_extension_array=True)
    pandas_df.set_index(ID_COLUMN, inplace=True)
    return pandas_df


def load_object_frame(path: Union[Iterable[PathType], PathType]) -> dd.DataFrame:
    """Load the "object" dataframe from a ZTF DR datafile.

    It loads all the columns but those that represent light curves.

    Parameters
    ----------
    path : single path or iterable of paths
        Path to the datafile or files to load. If a single path is given, it
        should be a directory of `.parquet` files. If an iterator is given, it
        should yield paths to `.parquet` files.

    Returns
    -------
    dd.DataFrame
        A lazily loaded Dask dataframe with the "object" table.
    """
    return load_frame_from_path(load_object_df, path, meta=None)


def load_source_frame(path: Union[Iterable[PathType], PathType]) -> dd.DataFrame:
    """Load the "source" dataframe from a ZTF DR datafile.

    It loads objectid column and columns representing light curves.

    Parameters
    ----------
    path : single path or iterable of paths
        Path to the datafile or files to load. If a single path is given, it
        should be a directory of `.parquet` files. If an iterator is given, it
        should yield paths to `.parquet` files.

    Returns
    -------
    dd.DataFrame
        A lazily loaded Dask dataframe with the "source" table.
    """
    return load_frame_from_path(load_source_df, path, meta=None)


def load_frame_from_path(
        func: Callable[[PathType], pd.DataFrame],
        path: Union[Iterable[PathType], PathType],
        meta: Optional[pd.DataFrame]
) -> dd.DataFrame:
    """Load a dataframe from a ZTF DR datafile applying a function to files

    Parameters
    ----------
    func : function of Path or str -> pd.DataFrame
        Function to apply to each file to load the dataframe. Its signature is
        `fn(path: Path | str) -> pd.DataFrame`, so it gets a `Path` object pointing
        to a parquet file and should return a pandas dataframe.

    path :  single path or iterable of paths
        Path to the datafile or files to load. If a single path is given, it
        should be a directory of `.parquet` files. If an iterator is given, it
        should yield paths to `.parquet` files.

    meta : pd.DataFrame or None
        Empty dataframe with the expected schema of the resulting dataframe,
        see this blog post for details
        https://blog.dask.org/2022/08/09/understanding-meta-keyword-argument
        If `None`, the schema will be inferred from the first file.

    Returns
    -------
    dd.DataFrame
        A lazily loaded Dask dataframe.
    """
    if isinstance(path, PathType.__args__):  # type: ignore
        path = Path(path).glob('**/*.parquet')
    path = cast(Iterable[PathType], path)

    ordered_paths = order_paths_by_oid(path)
    divisions = derive_dd_divisions(ordered_paths)

    if meta is None:
        meta = func(ordered_paths[0]).iloc[:0]

    return dd.from_map(
        func,
        ordered_paths,
        meta=meta,
        divisions=divisions,
    )


def derive_dd_divisions(ordered_paths: Iterable[PathType]) -> Tuple[int, ...]:
    """Derive Dask Dataframe divisions from a list of paths ordered by OID.

    Parameters
    ----------
    ordered_paths : iterable of Path or str
        Iterable of paths to parquet files ordered by OID. For example,
        the output of `order_paths_by_oid`.

    Returns
    -------
    Tuple[int]
        A tuple of integers representing the divisions of a Dask dataframe,
        n+1 integers for n paths. See
        https://docs.dask.org/en/latest/dataframe-design.html#partitions
    """

    divisions = []
    for path in ordered_paths:
        parsed_path = ParsedDataFilePath.from_path(path)
        divisions.append(parsed_path.start_oid)
    if len(divisions) == 0:
        raise ValueError('No paths given')
    divisions.append(parsed_path.stop_oid)

    return tuple(divisions)

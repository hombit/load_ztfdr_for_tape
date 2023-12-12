from pathlib import Path
from typing import Iterable, Union

import pandas as pd
import polars as pl

from load_ztfdr_for_tape.columns import (ID_COLUMN, OBJECT_COLUMNS,
                                         SOURCE_COLUMNS)

__all__ = ["load_object_df", "load_source_df"]


def load_object_df(path: Union[str, Path], columns: Iterable[str] = OBJECT_COLUMNS) -> pd.DataFrame:
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
    polars_df = pl.read_parquet(path, columns=[ID_COLUMN] + list(columns))
    pandas_df = polars_df.to_pandas(use_pyarrow_extension_array=True)
    pandas_df.set_index(ID_COLUMN, inplace=True)
    return pandas_df


def load_source_df(path: Union[str, Path], columns: Iterable[str] = SOURCE_COLUMNS) -> pd.DataFrame:
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

    polars_nested_df = pl.read_parquet(path, columns=[ID_COLUMN] + columns)
    polars_flat_df = polars_nested_df.explode(columns)
    pandas_df = polars_flat_df.to_pandas(use_pyarrow_extension_array=True)
    pandas_df.set_index(ID_COLUMN, inplace=True)
    return pandas_df

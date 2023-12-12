from pandas.api.types import is_numeric_dtype

from load_ztfdr_for_tape import columns, pandas


def test_load_object_df(lc_dr19_single_file):
    df = pandas.load_object_df(lc_dr19_single_file)
    assert not df.empty
    assert df.index.name == pandas.ID_COLUMN
    assert set(df.columns) == set(columns.OBJECT_COLUMNS)
    for column, dtype in zip(df.columns, df.dtypes):
        assert is_numeric_dtype(dtype), f"Column {column} is not numeric, but {dtype}"


def test_load_source_df(lc_dr19_single_file):
    df = pandas.load_source_df(lc_dr19_single_file)
    assert not df.empty
    assert df.index.name == pandas.ID_COLUMN
    assert set(df.columns) == set(columns.SOURCE_COLUMNS)
    for column, dtype in zip(df.columns, df.dtypes):
        assert is_numeric_dtype(dtype), f"Column {column} is not numeric, but {dtype}"

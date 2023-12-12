# load_ztfdr_for_tape

[![Template](https://img.shields.io/badge/Template-LINCC%20Frameworks%20Python%20Project%20Template-brightgreen)](https://lincc-ppt.readthedocs.io/en/latest/)

[![PyPI](https://img.shields.io/pypi/v/load_ztfdr_for_tape?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/load_ztfdr_for_tape/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/hombit/load_ztfdr_for_tape/smoke-test.yml)](https://github.com/hombit/load_ztfdr_for_tape/actions/workflows/smoke-test.yml)
[![codecov](https://codecov.io/gh/hombit/load_ztfdr_for_tape/branch/main/graph/badge.svg)](https://codecov.io/gh/hombit/load_ztfdr_for_tape)
[![Read the Docs](https://img.shields.io/readthedocs/load_ztfdr_for_tape)](https://load_ztfdr_for_tape.readthedocs.io/)
[![benchmarks](https://img.shields.io/github/actions/workflow/status/hombit/load_ztfdr_for_tape/asv-main.yml?label=benchmarks)](https://hombit.github.io/load_ztfdr_for_tape/)

This project was automatically generated using the LINCC-Frameworks 
[python-project-template](https://github.com/lincc-frameworks/python-project-template).

Get Dask DataFrames from ZTF DRs for [LINCC Frameworks' Tape](https://github.com/lincc-frameworks/tape/).
Basically, you need a single function call to get "object" (metadata) and "source" (photometry per detection) tables from a ZTF DR:

```python
from load_ztfdr_for_tape import load_object_source_frames_from_path
from tape import Ensemble, ColumnMapper

# Replace with the actual path, here we use few files from the test data
ztf_dr_path = './tests/data/lc_dr19'
objects, sources = load_object_source_frames_from_path(ztf_dr_path)
column_mapper = ColumnMapper(
    id_col='objectid',
    time_col='hmjd',
    flux_col='mag',
    err_col='magerr',
    band_col='filterid',
)

# Replace `False` with dask.distributed.Client instance for parallel execution
ens = Ensemble(client=False)
ens.from_dask_dataframe(
    object_frame=objects,
    source_frame=sources,
    column_mapper=column_mapper,
    # Do not make an initial sync of the tables
    sync_tables=False,
    # We did sort the tables by objectid
    sorted=True,
    sort=False,
)
```

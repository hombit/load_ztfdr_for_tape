from pathlib import Path

import pytest

TEST_DIR = Path(__file__).parent


@pytest.fixture
def lc_dr19():
    return TEST_DIR / 'data' / 'lc_dr19'


@pytest.fixture
def lc_dr19_field001518(lc_dr19):
    return lc_dr19 / '1' / 'field001518'


@pytest.fixture
def lc_dr19_field000202(lc_dr19):
    return lc_dr19 / '0' / 'field000202'


@pytest.fixture
def lc_dr19_single_file(lc_dr19):
    return lc_dr19 / '0' / 'field000202' / 'ztf_000202_zg_c12_q1_dr19.parquet'

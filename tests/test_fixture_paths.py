"""Test the paths defined in conftest.py as fixtures."""


def test_lc_dr19_exists(lc_dr19):
    assert lc_dr19.exists(), "Directory does not exist"


def test_lc_dr19_field001518_exists(lc_dr19_field001518):
    assert lc_dr19_field001518.exists(), "Directory does not exist"


def test_lc_dr19_field000202_exists(lc_dr19_field000202):
    assert lc_dr19_field000202.exists(), "Directory does not exist"

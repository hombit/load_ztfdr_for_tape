"""Two sample benchmarks to compute runtime and memory usage.

For more information on writing benchmarks:
https://asv.readthedocs.io/en/stable/writing_benchmarks.html."""

from load_ztfdr_for_tape.oid import OIDParts


def time_computation():
    """Time computations are prefixed with 'time'."""
    OIDParts.from_oid(687311400069813)


def mem_list():
    """Memory computations are prefixed with 'mem' or 'peakmem'."""
    OIDParts.from_oid(687311400069813)

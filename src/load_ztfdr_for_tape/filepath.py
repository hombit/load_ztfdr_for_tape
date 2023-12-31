"""Tools to parse ZTF Data Release file paths"""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Union

from load_ztfdr_for_tape.bands import ZTF_BAND_NAMES
from load_ztfdr_for_tape.oid import OIDParts

__all__ = ['ParsedDataFilePath', 'get_ordered_paths', 'order_paths_by_oid']


@dataclass
class ParsedDataFilePath:
    """Parsed ZTF DR Datafile path"""

    field: int
    band: str
    ccdid: int
    qid: int
    dr: int

    @classmethod
    def from_path(cls, path: Union[str, Path]) -> 'ParsedDataFilePath':
        """Parse a ZTF DR Datafile path into its components."""
        path = Path(path)
        if path.suffix != '.parquet':
            raise ValueError(f'Expected .parquet file, got {path}')

        ztf, field_str, band, ccdid_str, qid_str, dr_str = path.stem.split('_')

        if ztf != 'ztf':
            raise ValueError(f'Expected filename to start with ztf, got {path}')

        try:
            field = int(field_str)
        except ValueError as e:
            raise ValueError(f'Expected field to be an integer, got {path}') from e

        if band not in ZTF_BAND_NAMES:
            raise ValueError(f'Expected band to be one of {ZTF_BAND_NAMES}, got {path}')

        if not ccdid_str.startswith('c'):
            raise ValueError(f'Expected ccdid to start with "c", got {path}')
        try:
            ccdid = int(ccdid_str.removeprefix('c'))
        except ValueError as e:
            raise ValueError(f'Expected ccdid to be an integer, got {path}') from e

        if not qid_str.startswith('q'):
            raise ValueError(f'Expected qid to start with "q", got {path}')
        try:
            qid = int(qid_str.removeprefix('q'))
        except ValueError as e:
            raise ValueError(f'Expected qid to be an integer, got {path}') from e

        if not dr_str.startswith('dr'):
            raise ValueError(f'Expected dr to start with "dr", got {path}')
        try:
            dr = int(dr_str.removeprefix('dr'))
        except ValueError as e:
            raise ValueError(f'Expected dr to be an integer, got {path}') from e

        return cls(field, band, ccdid, qid, dr)

    @property
    def start_oid(self) -> int:
        """Minimum possible OID for this file"""
        oid_parts = OIDParts(self.field, self.band, self.ccdid, self.qid, 0)  # type: ignore
        return oid_parts.oid

    @property
    def stop_oid(self) -> int:
        """Maximum possible OID for this file plus one"""
        oid_parts = OIDParts(self.field, self.band, self.ccdid, self.qid + 1, 0)  # type: ignore
        return oid_parts.oid


def order_paths_by_oid(paths: Iterable[Union[str, Path]]) -> List[Union[str, Path]]:
    """Order a list of paths by their OID."""
    return sorted(paths, key=lambda path: ParsedDataFilePath.from_path(path).start_oid)


def get_ordered_paths(directory: Union[str, Path]) -> List[Union[str, Path]]:
    """Get a list of paths in a directory ordered by their OID.

    It selects all parquet files and orders them in ascending order by their
    OID. This is useful when needed to get a list of files of the while
    ZTF DR in order of object IDs.
    """
    ordered = order_paths_by_oid(Path(directory).glob('**/*.parquet'))
    if len(ordered) == 0:
        raise ValueError(f'No parquet files found in {directory}')
    return ordered

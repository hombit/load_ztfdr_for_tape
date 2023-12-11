"""Tools to work with ZTF Data Relase Object IDs (OIDs)."""

from dataclasses import dataclass
from typing import Self, cast

__all__ = ['OIDParts']


@dataclass
class OIDParts:
    """Object ID broken down into its parts.

    `band` may be specified as a number (1,2,3), a single char (g,r,i) or
    a band name (zg,zr,zi). It is stored as a number internally.
    """

    field: int
    band: int
    ccdid: int
    qid: int
    counter: int

    COUNTER_OFFSET_DIGITS = 0
    QID_OFFSET_DIGITS = COUNTER_OFFSET_DIGITS + 8
    CCDID_OFFSET_DIGITS = QID_OFFSET_DIGITS + 1
    BAND_OFFSET_DIGITS = CCDID_OFFSET_DIGITS + 2
    FIELD_OFFSET_DIGITS = BAND_OFFSET_DIGITS + 1

    def __post_init__(self):
        # Convert band to a number if it is a single char or a band name.
        if isinstance(self.band, str):
            band = cast(str, self.band)
            band = band.removeprefix('z')
            self.band = 'gri'.index(band) + 1

    @classmethod
    def from_oid(cls, oid: int) -> Self:
        """Create OIDParts from an OID integer."""
        field, rest = divmod(oid, 10 ** cls.FIELD_OFFSET_DIGITS)
        band, rest = divmod(rest, 10 ** cls.BAND_OFFSET_DIGITS)
        ccdid, rest = divmod(rest, 10 ** cls.CCDID_OFFSET_DIGITS)
        qid, counter = divmod(rest, 10 ** cls.QID_OFFSET_DIGITS)
        return cls(field, band, ccdid, qid, counter)

    @property
    def oid(self) -> int:
        """Object ID as a single integer.

        It should always fit into UInt64, we use Python's int for simplicity.
        """
        return (
            self.field * 10 ** self.FIELD_OFFSET_DIGITS
            + self.band * 10 ** self.BAND_OFFSET_DIGITS
            + self.ccdid * 10 ** self.CCDID_OFFSET_DIGITS
            + self.qid * 10 ** self.QID_OFFSET_DIGITS
            + self.counter * 10 ** self.COUNTER_OFFSET_DIGITS
        )

    @property
    def band_char(self) -> str:
        """Single-character band name, one of g,r,i."""
        return 'gri'[self.band - 1]

    @property
    def band_name(self) -> str:
        """ZTF DR band name, one of zg,zr,zi."""
        return f'z{self.band_char}'

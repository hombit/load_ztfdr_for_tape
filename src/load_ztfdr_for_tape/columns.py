ID_COLUMN = 'objectid'
"""Name of the primary index column."""

# We skip 'filterid' and use it for the source table instead.
OBJECT_COLUMNS = ('fieldid', 'rcid', 'objra', 'objdec', 'nepochs')
"""Names of the columns representing the object metadata."""

TIME_DOMAIN_COLUMNS = ('hmjd', 'mag', 'magerr', 'clrcoeff', 'catflags')
"""Names of the columns representing photometric data, they are nested arrays."""

SOURCE_COLUMNS = ('filterid',) + TIME_DOMAIN_COLUMNS
"""Names of the columns representing the detection data."""

UNUSED_COLUMNS = ('__index_level_0__',)
"""Names of the columns we want to ignore."""

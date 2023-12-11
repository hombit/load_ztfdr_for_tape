from load_ztfdr_for_tape.oid import OIDParts


def test_oid_parts_0_sky_partitioning():
    """Test OIDParts.from_oid for a three-digit field OID."""

    oid = 633207400004730

    field = 633
    band = 2
    band_char = 'r'
    band_name = 'zr'
    rcid = 27
    ccdid = rcid // 4 + 1
    qid = rcid % 4 + 1
    counter = 4730

    oid_parts = OIDParts.from_oid(oid)

    assert oid_parts.field == field
    assert oid_parts.band == band
    assert oid_parts.band_char == band_char
    assert oid_parts.band_name == band_name
    assert oid_parts.ccdid == ccdid
    assert oid_parts.qid == qid
    assert oid_parts.counter == counter
    assert oid_parts.oid == oid


def test_oid_parts_1_sky_partitioning():
    """Test OIDParts.from_oid for a four-digit field OID."""

    oid = 1722107400005560

    field = 1722
    band = 1
    band_char = 'g'
    band_name = 'zg'
    rcid = 27
    ccdid = rcid // 4 + 1
    qid = rcid % 4 + 1
    counter = 5560

    oid_parts = OIDParts.from_oid(oid)

    assert oid_parts.field == field
    assert oid_parts.band == band
    assert oid_parts.band_char == band_char
    assert oid_parts.band_name == band_name
    assert oid_parts.ccdid == ccdid
    assert oid_parts.qid == qid
    assert oid_parts.counter == counter


def test_from_char_band():
    """Test OIDParts construction with a string band."""

    oid_parts = OIDParts(633, 'r', 7, 4, 0)
    assert oid_parts.band == 2
    assert oid_parts.oid == 633207400000000


def test_from_str_band():
    """Test OIDParts construction with a string band."""

    oid_parts = OIDParts(633, 'zr', 7, 4, 0)
    assert oid_parts.band == 2
    assert oid_parts.oid == 633207400000000

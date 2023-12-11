from load_ztfdr_for_tape.filepath import ParsedDataFilePath


def test_parse_file_path():
    path = '0/field000695/ztf_000695_zr_c16_q3_dr19.parquet'
    parsed = ParsedDataFilePath.from_path(path)

    assert parsed.field == 695
    assert parsed.band == 'zr'
    assert parsed.ccdid == 16
    assert parsed.qid == 3
    assert parsed.dr == 19
    assert parsed.start_oid == 695216300000000

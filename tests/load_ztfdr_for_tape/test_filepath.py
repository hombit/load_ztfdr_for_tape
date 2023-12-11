from load_ztfdr_for_tape.filepath import ParsedDataFilePath, order_paths_by_oid


def test_parse_file_path():
    path = '0/field000695/ztf_000695_zr_c16_q3_dr19.parquet'
    parsed = ParsedDataFilePath.from_path(path)

    assert parsed.field == 695
    assert parsed.band == 'zr'
    assert parsed.ccdid == 16
    assert parsed.qid == 3
    assert parsed.dr == 19
    assert parsed.start_oid == 695216300000000
    assert parsed.stop_oid == 695216400000000


def test_order_paths_by_oid():
    paths = [
        'ztf_000695_zg_c07_q4_dr19.parquet',
        'ztf_000695_zg_c01_q1_dr19.parquet',
        'ztf_000695_zg_c01_q2_dr19.parquet',
        'ztf_000695_zi_c15_q4_dr19.parquet',
        'ztf_000695_zi_c16_q3_dr19.parquet',
        'ztf_000695_zr_c15_q4_dr19.parquet',
        'ztf_000695_zr_c01_q1_dr19.parquet',
    ]

    ordered = order_paths_by_oid(paths)

    assert ordered == [
        'ztf_000695_zg_c01_q1_dr19.parquet',
        'ztf_000695_zg_c01_q2_dr19.parquet',
        'ztf_000695_zg_c07_q4_dr19.parquet',
        'ztf_000695_zr_c01_q1_dr19.parquet',
        'ztf_000695_zr_c15_q4_dr19.parquet',
        'ztf_000695_zi_c15_q4_dr19.parquet',
        'ztf_000695_zi_c16_q3_dr19.parquet',
    ]

from load_ztfdr_for_tape import bands


def test_len():
    length = None

    for attr in dir(bands):
        if attr.startswith('_'):
            continue

        if length is None:
            length = len(getattr(bands, attr))
        else:
            assert len(getattr(bands, attr)) == length

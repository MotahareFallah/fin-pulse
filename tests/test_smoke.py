import fin_pulse


def test_package_exposes_version():
    assert isinstance(fin_pulse.__version__, str)

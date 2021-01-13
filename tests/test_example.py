from gbq import greeting


def test_greeting():
    assert greeting() == "hello, world"

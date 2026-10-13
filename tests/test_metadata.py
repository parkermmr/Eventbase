from eventbase.metadata import decode, encode


def test_round_trip():
    metadata = {"id": 7, "tags": ["a", "b"], "nested": {"x": None}, "name": "report"}
    encoded = encode(metadata)
    assert all(isinstance(value, str) for value in encoded.values())
    assert decode(encoded.items()) == metadata


def test_foreign_values_stay_strings():
    pairs = [("plain", b"hello world"), ("number", b"12"), ("empty", None)]
    assert decode(pairs) == {"plain": "hello world", "number": 12, "empty": None}

from gold_daily_merchant import input_id


def test_gold_input_fingerprint_tracks_snapshots_and_is_order_independent():
    assert input_id({"a": 1, "b": 2}) == input_id({"b": 2, "a": 1})
    assert input_id({"a": 1, "b": 2}) != input_id({"a": 1, "b": 3})

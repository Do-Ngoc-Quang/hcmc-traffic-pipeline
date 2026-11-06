import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import select_points as sp


def cand(i, lat, lon, zone="center"):
    return {"id": i, "name": i, "lat": lat, "lon": lon, "zone": zone}


def row(conf, speed):
    return {"confidence": conf, "current_speed": speed}


def test_evaluate_rejects_low_confidence_flat_speed_and_errors():
    cs = [cand("a", 10.77, 106.70), cand("b", 10.80, 106.71), cand("c", 10.85, 106.72), cand("d", 10.9, 106.7)]
    p = [
        {"a": row(0.9, 40), "b": row(0.9, 30), "c": row(0.9, 30), "d": {"error": "x"}},
        {"a": row(0.9, 20), "b": row(0.5, 20), "c": row(0.9, 31), "d": row(0.9, 20)},
        {"a": row(0.9, 35), "b": row(0.9, 25), "c": row(0.9, 30), "d": row(0.9, 20)},
    ]
    ok, rej = sp.evaluate(cs, p)
    assert [c["id"] for c in ok] == ["a"]
    assert set(rej) == {"b", "c", "d"}


def test_choose_one_point_per_cell_and_covers_zones():
    a = {**cand("a", 10.7769, 106.7009, "center"), "speed_range": 30}
    a2 = {**cand("a2", 10.77691, 106.70091, "center"), "speed_range": 20}   # cùng ô r8 với a
    n = {**cand("n", 10.85, 106.65, "north"), "speed_range": 5}
    chosen = sp.choose([a, a2, n], n=10)
    assert [c["id"] for c in chosen] == ["a", "n"]

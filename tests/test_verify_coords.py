import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import verify_coords as vc


def test_haversine_one_degree_latitude_is_about_111km():
    assert abs(vc.haversine_m(10.0, 106.0, 11.0, 106.0) - 111_195) < 300


def test_classify_ok_check_out():
    e = {"lat": 10.7686, "lon": 106.6942}
    near = {"lat": 10.7690, "lon": 106.6945}
    far = {"lat": 10.78, "lon": 106.70}
    assert vc.classify(e, near, 300)[0] == "OK"
    assert vc.classify(e, far, 300)[0] == "CHECK"
    assert vc.classify(e, None, 300) == ("CHECK", None)
    assert vc.classify({"lat": 21.0, "lon": 105.8}, near, 300) == ("OUT", None)


def test_verify_uses_cache_and_survives_errors(tmp_path):
    calls = []

    def fake(path, params):
        calls.append(path)
        if path == "reverse":
            if params["lat"] == 1.0:
                raise OSError("mạng lỗi")
            return {"address": {"road": "Nguyễn Thái Học"}}
        return [{"lat": "10.7688", "lon": "106.6944", "display_name": "x"}]

    geo = vc.Geocoder(fetch=fake, cache_path=tmp_path / "c.json", sleep=lambda s: None)
    entries = [{"id": "a", "name": "A", "lat": 10.7686, "lon": 106.6942},
               {"id": "b", "name": "B", "lat": 1.0, "lon": 1.0}]
    rows = vc.verify(entries, geo, 300)
    assert rows[0]["status"] == "OK" and rows[0]["road_at_point"] == "Nguyễn Thái Học"
    assert rows[1]["status"] == "ERROR"
    n = len(calls)
    vc.verify(entries[:1], geo, 300)
    assert len(calls) == n          # lần chạy lại đọc từ cache

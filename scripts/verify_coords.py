#!/usr/bin/env python3
"""Đối chiếu toạ độ trong file yaml với OpenStreetMap (Nominatim).

Với mỗi mục có `name`, `lat`, `lon`, script làm hai việc:
  - reverse geocode toạ độ hiện có để biết nó nằm trên đường nào;
  - geocode theo tên để lấy toạ độ gợi ý, rồi tính khoảng cách tới toạ độ hiện có.

Trạng thái mỗi mục:
  OK         toạ độ nằm trong TP.HCM và gần toạ độ geocode theo tên
  CHECK      lệch quá ngưỡng, hoặc không geocode được theo tên (tên nút giao thường không có trong OSM)
  OUT        toạ độ nằm ngoài khung TP.HCM

Dùng cho cả config/candidates.yaml (khoá `candidates`) và config/points.yaml (khoá `points`).
Nominatim giới hạn 1 request/giây nên script chờ giữa các lần gọi và lưu cache để chạy lại không tốn request.
Dữ liệu OSM là dữ liệu mở (ODbL), khác với dữ liệu TomTom nên được phép cache.
"""
import argparse, json, math, sys, time, urllib.parse, urllib.request
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent.parent
BASE = "https://nominatim.openstreetmap.org"
USER_AGENT = "hcmc-traffic-pipeline-verify/0.1"
CACHE_FILE = ROOT / "data" / "geocode_cache.json"
# Khung gần đúng của TP.HCM (sau sáp nhập có thể rộng hơn, chỉnh nếu cần): min_lon, min_lat, max_lon, max_lat.
BBOX = (106.35, 10.35, 107.05, 11.20)
CITY_SUFFIX = ", Hồ Chí Minh, Việt Nam"


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def in_bbox(lat, lon, bbox=BBOX):
    return bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]


def classify(entry, suggested, max_offset_m):
    """Trả về (trạng thái, khoảng cách m hoặc None). Hàm thuần để dễ test."""
    if not in_bbox(entry["lat"], entry["lon"]):
        return "OUT", None
    if suggested is None:
        return "CHECK", None
    d = haversine_m(entry["lat"], entry["lon"], suggested["lat"], suggested["lon"])
    return ("OK" if d <= max_offset_m else "CHECK"), d


def http_get_json(path, params):
    url = f"{BASE}/{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "vi"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


class Geocoder:
    def __init__(self, fetch=http_get_json, cache_path=CACHE_FILE, delay=1.1, sleep=time.sleep):
        self.fetch, self.cache_path, self.delay, self.sleep = fetch, cache_path, delay, sleep
        self.cache = json.loads(cache_path.read_text()) if cache_path and cache_path.exists() else {}

    def _call(self, key, path, params):
        if key not in self.cache:
            self.sleep(self.delay)
            self.cache[key] = self.fetch(path, params)
            if self.cache_path:
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False))
        return self.cache[key]

    def reverse(self, lat, lon):
        res = self._call(f"r:{lat:.5f},{lon:.5f}", "reverse",
                         {"format": "jsonv2", "lat": lat, "lon": lon, "zoom": 17, "addressdetails": 1})
        addr = res.get("address", {}) if isinstance(res, dict) else {}
        return addr.get("road") or addr.get("neighbourhood") or res.get("name") or ""

    def forward(self, name):
        res = self._call(f"f:{name}", "search", {"format": "jsonv2", "q": name + CITY_SUFFIX, "limit": 1})
        if not res:
            return None
        return {"lat": float(res[0]["lat"]), "lon": float(res[0]["lon"]), "label": res[0].get("display_name", "")}


def load_entries(path):
    data = yaml.safe_load(Path(path).read_text(encoding="utf8"))
    for key in ("candidates", "points"):
        if key in data:
            return [e for e in data[key] if e.get("lat") and e.get("lon")]
    sys.exit(f"{path} không có khoá candidates hoặc points")


def verify(entries, geo, max_offset_m):
    rows = []
    for e in entries:
        try:
            road = geo.reverse(e["lat"], e["lon"])
            sug = geo.forward(e["name"])
        except Exception as ex:  # lỗi mạng của một mục không làm hỏng cả lượt
            rows.append({"id": e["id"], "name": e["name"], "status": "ERROR", "error": str(ex)})
            continue
        status, dist = classify(e, sug, max_offset_m)
        rows.append({"id": e["id"], "name": e["name"], "status": status, "road_at_point": road,
                     "offset_m": None if dist is None else round(dist),
                     "suggested": None if sug is None else {"lat": sug["lat"], "lon": sug["lon"]}})
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--file", default=str(ROOT / "config" / "candidates.yaml"))
    ap.add_argument("--max-offset-m", type=float, default=300)
    ap.add_argument("--json", help="ghi kết quả đầy đủ ra file json")
    ap.add_argument("--strict", action="store_true", help="thoát với mã 1 nếu còn mục khác OK")
    a = ap.parse_args()
    rows = verify(load_entries(a.file), Geocoder(), a.max_offset_m)
    for r in rows:
        extra = f"đường tại điểm: {r.get('road_at_point') or '?'}"
        if r["status"] == "ERROR":
            extra = r["error"]
        elif r["offset_m"] is not None:
            extra += f"; lệch {r['offset_m']} m so với geocode theo tên"
        elif r["status"] == "CHECK":
            extra += "; không geocode được theo tên"
        print(f"{r['status']:<6} {r['id']:<4} {r['name']} | {extra}")
    if a.json:
        Path(a.json).write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    bad = sum(r["status"] != "OK" for r in rows)
    print(f"{len(rows) - bad}/{len(rows)} OK")
    sys.exit(1 if a.strict and bad else 0)


if __name__ == "__main__":
    main()

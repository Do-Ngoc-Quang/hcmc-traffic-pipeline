#!/usr/bin/env python3
"""Chọn 10 điểm đo giao thông từ danh sách ứng viên (mục 4 của thiết kế).

  probe  --slot am|noon|pm   gọi Flow Segment Data cho mọi ứng viên, ghi kết quả vào data/probe/
  select                     đọc kết quả 3 khung giờ, lọc theo confidence và độ biến động,
                             chọn tối đa 10 điểm và ghi gợi ý vào config/points.suggested.yaml

Chi phí: số ứng viên x 3 request (khoảng 60 request cho 20 ứng viên).
Cần TOMTOM_API_KEY trong môi trường. Tên trường của response theo tài liệu, chưa đối chiếu
với response thật; xem docs/design.md mục 5.
"""
import argparse, json, os, sys, urllib.request
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent.parent
PROBE_DIR = ROOT / "data" / "probe"   # đã nằm trong .gitignore, xoá sau khi chọn điểm xong
SLOTS = ("am", "noon", "pm")
URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom}/json?point={lat},{lon}&key={key}"
MIN_CONFIDENCE = 0.7      # lựa chọn thiết kế, chỉnh theo kết quả thực tế
MIN_SPEED_RANGE = 5.0     # km/h chênh lệch tối thiểu giữa các khung giờ (giả định, chỉnh được)
ZONES = ("center", "north", "south", "east", "west")


def load_candidates():
    data = yaml.safe_load((ROOT / "config" / "candidates.yaml").read_text())
    return [c for c in data["candidates"] if c["lat"] and c["lon"]]


def fetch(c, key, zoom=10):
    url = URL.format(zoom=zoom, lat=c["lat"], lon=c["lon"], key=key)
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)["flowSegmentData"]


def probe(slot):
    key = os.environ.get("TOMTOM_API_KEY")
    if not key:
        sys.exit("Thiếu TOMTOM_API_KEY")
    PROBE_DIR.mkdir(parents=True, exist_ok=True)
    out = {}
    for c in load_candidates():
        try:
            f = fetch(c, key)
            out[c["id"]] = {"confidence": f["confidence"], "current_speed": f["currentSpeed"],
                            "free_flow_speed": f["freeFlowSpeed"], "frc": f.get("frc")}
        except Exception as e:  # ghi lỗi để biết điểm nào không có dữ liệu
            out[c["id"]] = {"error": str(e)}
    (PROBE_DIR / f"{slot}.json").write_text(json.dumps(out, indent=2))
    print(f"Đã ghi {len(out)} ứng viên vào data/probe/{slot}.json")


def evaluate(cands, probes):
    """Trả về (đạt, loại) với lý do loại. Hàm thuần để dễ test."""
    ok, rejected = [], {}
    for c in cands:
        rows = [p.get(c["id"], {}) for p in probes]
        if any("error" in r or not r for r in rows):
            rejected[c["id"]] = "lỗi hoặc thiếu dữ liệu ở ít nhất một khung giờ"
            continue
        if min(r["confidence"] for r in rows) < MIN_CONFIDENCE:
            rejected[c["id"]] = f"confidence < {MIN_CONFIDENCE}"
            continue
        speeds = [r["current_speed"] for r in rows]
        if max(speeds) - min(speeds) < MIN_SPEED_RANGE:
            rejected[c["id"]] = "tốc độ ít thay đổi giữa các khung giờ"
            continue
        ok.append({**c, "speed_range": max(speeds) - min(speeds)})
    return ok, rejected


def choose(ok, n=10, res=8):
    """Mỗi ô r8 tối đa một điểm, ưu tiên phủ đủ vùng, rồi theo độ biến động tốc độ."""
    import h3
    ok = sorted(ok, key=lambda c: -c["speed_range"])
    chosen, cells = [], set()

    def take(c):
        cell = h3.latlng_to_cell(c["lat"], c["lon"], res)
        if cell in cells or c in chosen:
            return
        cells.add(cell); chosen.append({**c, "h3_r8": cell})

    for z in ZONES:                      # vòng 1: mỗi vùng ít nhất 1 điểm
        for c in ok:
            if c["zone"] == z:
                take(c); break
    for c in ok:                         # vòng 2: điền cho đủ n
        if len(chosen) >= n:
            break
        take(c)
    return chosen[:n]


def select():
    probes = []
    for s in SLOTS:
        f = PROBE_DIR / f"{s}.json"
        if not f.exists():
            sys.exit(f"Thiếu {f}; chạy probe --slot {s} trước")
        probes.append(json.loads(f.read_text()))
    ok, rejected = evaluate(load_candidates(), probes)
    chosen = choose(ok)
    for cid, why in rejected.items():
        print(f"loại {cid}: {why}")
    points = [{"id": f"p{i:02d}", "name": c["name"], "lat": c["lat"], "lon": c["lon"], "zone": c["zone"],
               "priority": 1, "min_confidence": MIN_CONFIDENCE, "zoom": 10}
              for i, c in enumerate(chosen, 1)]
    doc = {"city": "hcmc", "h3_res_traffic": 8, "h3_res_weather": 6,
           "incidents_bbox": ["min_lon", "min_lat", "max_lon", "max_lat"], "points": points}
    (ROOT / "config" / "points.suggested.yaml").write_text(yaml.safe_dump(doc, allow_unicode=True, sort_keys=False))
    print(f"Gợi ý {len(points)} điểm -> config/points.suggested.yaml (xem lại rồi đổi tên thành points.yaml)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("select")
    sub.add_parser("probe").add_argument("--slot", choices=SLOTS, required=True)
    a = ap.parse_args()
    select() if a.cmd == "select" else probe(a.slot)

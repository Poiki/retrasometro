"""Refresh the OpenStreetMap rail geometry used to draw detected sections."""

import gzip
import json
import math
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

target = Path(__file__).resolve().parents[1] / "src/data/railways.geojson"
query = '[out:json][timeout:180];way["railway"="rail"](35,-10,44.5,4.5);out geom;'
request = Request(
    "https://overpass-api.de/api/interpreter?" + urlencode({"data": query}),
    headers={"User-Agent": "retrasometro/1.0 (+https://retrasometro.com)", "Accept-Encoding": "gzip"},
)
with urlopen(request, timeout=200) as response:
    raw = response.read()
    if response.headers.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    payload = json.loads(raw)
if payload.get("remark"):
    raise RuntimeError(payload["remark"])

lines = []
for way in payload["elements"]:
    if way.get("type") != "way" or way.get("tags", {}).get("railway") != "rail":
        continue
    coordinates = []
    for point in way.get("geometry", []):
        lon, lat = point["lon"], point["lat"]
        if not (math.isfinite(lon) and math.isfinite(lat) and abs(lon) <= 180 and abs(lat) <= 85.05112878):
            raise ValueError("Invalid railway coordinates")
        coordinate = [round(lon, 5), round(lat, 5)]
        if not coordinates or coordinate != coordinates[-1]:
            coordinates.append(coordinate)
    if len(coordinates) >= 2:
        lines.append(coordinates)
assert lines, "No railway geometry returned; keeping the existing snapshot"
result = {
    "type": "Feature", "properties": {
        "source": "OpenStreetMap via Overpass API", "copyright": "© OpenStreetMap contributors",
        "license": "https://opendatacommons.org/licenses/odbl/1-0/",
        "snapshot": payload["osm3s"]["timestamp_osm_base"], "query": query,
    },
    "geometry": {"type": "MultiLineString", "coordinates": lines},
}
target.parent.mkdir(parents=True, exist_ok=True)
temporary = target.with_suffix(".tmp")
temporary.write_text(json.dumps(result, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
temporary.replace(target)
print(f"Saved {len(lines)} railway ways, {sum(map(len, lines))} points, {target.stat().st_size:,} bytes")

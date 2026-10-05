"""
James River centerline helpers.

river_centerline.csv = the James River main channel through Richmond (Huguenot area to
below Ancarrow's Landing), taken from OpenStreetMap (waterway=river, name="James River"),
86 points. Map data (c) OpenStreetMap contributors, ODbL - credit it on the dashboard.

Used to (1) place simulated incidents ON the water, and (2) reject parsed points that
are not near the river.
"""
import math
from functools import lru_cache
from pathlib import Path

import pandas as pd

_LAT0 = 37.53
_MX = math.cos(math.radians(_LAT0)) * 111_320  # metres per degree longitude here
_MY = 110_540                                   # metres per degree latitude


def _xy(lat, lon):
    return lon * _MX, lat * _MY


def _ll(x, y):
    return y / _MY, x / _MX


@lru_cache(maxsize=1)
def line():
    df = pd.read_csv(Path(__file__).with_name("river_centerline.csv"))
    pts = [_xy(a, b) for a, b in zip(df.latitude, df.longitude)]
    cum = [0.0]
    for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
        cum.append(cum[-1] + math.hypot(x2 - x1, y2 - y1))
    return pts, cum


def project(lat, lon):
    """Return (distance along the river in m, distance from the centerline in m)."""
    pts, cum = line()
    px, py = _xy(lat, lon)
    best = (float("inf"), 0.0)
    for i, ((ax, ay), (bx, by)) in enumerate(zip(pts, pts[1:])):
        dx, dy = bx - ax, by - ay
        seg2 = dx * dx + dy * dy or 1.0
        t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / seg2))
        d = math.hypot(px - ax - t * dx, py - ay - t * dy)
        if d < best[0]:
            best = (d, cum[i] + t * math.sqrt(seg2))
    return best[1], best[0]


def point_at(chainage, offset_m=0.0):
    """Point `chainage` metres along the river, shifted `offset_m` sideways."""
    pts, cum = line()
    chainage = max(0.0, min(cum[-1], chainage))
    for i in range(len(cum) - 1):
        if cum[i + 1] >= chainage:
            (ax, ay), (bx, by) = pts[i], pts[i + 1]
            L = (cum[i + 1] - cum[i]) or 1.0
            t = (chainage - cum[i]) / L
            ux, uy = (bx - ax) / L, (by - ay) / L
            x, y = ax + t * (bx - ax) - uy * offset_m, ay + t * (by - ay) + ux * offset_m
            return _ll(x, y)
    return _ll(*pts[-1])


def distance_to_river_m(lat, lon):
    return project(lat, lon)[1]


def length_m():
    return line()[1][-1]

"""
One-off migration after task V-03 (site coordinates checked by hand, 2026-09-28).

Problem: the simulated incidents were placed on the river around the OLD, unchecked site
coordinates. Several were far off (Z-Dam was put downtown; it is the Williams Island dam
~9 km upstream. Bosher Dam, Pony Pasture and 42nd St moved 1.7-4.3 km). Leaving the old
points would put every Z-Dam what3words/GPS dot downtown while the Z-Dam site circle sits
upstream.

What this does, without touching anything the note-writers wrote except coordinates:
  1. Extends river_centerline.csv upstream to the checked Bosher Dam point (straight segment,
     ~1 km; the OSM extract stopped short of the dam - approximation, stated in README).
  2. Re-places each incident's true point around its NEW site coordinates, keeping its
     assigned site. Between-site incidents are re-drawn inside the stretch of river that is
     still nearest to their assigned site, so the site named in every note stays correct.
  3. Swaps the old what3words (mock) / GPS strings for new ones in BOTH narrative files,
     so notes stay word-for-word identical apart from those tokens.
  4. Recomputes gold_site for what3words/GPS rows (nearest site to the new true point).
Seeded; rerunning gives the same result only from the _pre_sitecheck copies.
"""
import random
import re

import pandas as pd

import river_geometry as rg
import w3w_client
from generate_narratives_llm import gold_for
from extract_locations import W3W_RE

SEED = 2026
rng = random.Random(SEED)
SITE_SPREAD_M, BANK_OFFSET_M = 150, 50   # same assumptions as generate_narratives.py

sites = pd.read_csv("river_sites.csv")

# 1. centerline extension to Bosher Dam
cl = pd.read_csv("river_centerline.csv")
b = sites[sites.site_id == "BOSH"].iloc[0]
if abs(cl.longitude.min() - b.longitude) > 1e-6:
    cl = pd.concat([pd.DataFrame({"latitude": [round(b.latitude, 6)], "longitude": [round(b.longitude, 6)]}),
                    cl[["latitude", "longitude"]]], ignore_index=True)
    cl.insert(0, "seq", range(len(cl)))
    cl.to_csv("river_centerline.csv", index=False)
    print(f"centerline extended to Bosher Dam: {len(cl)} points")

# 2. re-place true points
site_ch = {r.site_name: rg.project(r.latitude, r.longitude)[0] for r in sites.itertuples()}
order = sorted(site_ch, key=site_ch.get)
lo, hi = 0.0, rg.length_m()
interval = {}
for i, n in enumerate(order):
    left = lo if i == 0 else (site_ch[order[i - 1]] + site_ch[n]) / 2
    right = hi if i == len(order) - 1 else (site_ch[n] + site_ch[order[i + 1]]) / 2
    interval[n] = (left, right)

t = pd.read_csv("incidents_with_narratives.csv")
g = pd.read_csv("incidents_with_narratives_llm.csv")
assert (t.incident_id == g.incident_id).all()
cache = w3w_client.load_cache()
new_lat, new_lon, swaps = [], [], []
for r in t.itertuples():
    name = r.landmark_cross_street
    if r.between_sites_sim:
        a, z = interval[name]
        ch = rng.uniform(a, z)
    else:
        a, z = interval[name]
        ch = min(max(site_ch[name] + rng.gauss(0, SITE_SPREAD_M), lo), hi)
    lat, lon = rg.point_at(ch, rng.uniform(-BANK_OFFSET_M, BANK_OFFSET_M))
    lat, lon = round(lat, 6), round(lon, 6)
    new_lat.append(lat); new_lon.append(lon)
    # 3. token swaps
    pairs = []
    if r.location_format_true == "w3w":
        old = W3W_RE.search(r.firefighter_narrative).group(1)
        pairs.append((old, w3w_client.coords_to_words(lat, lon, cache, rng)))
    elif r.location_format_true == "gps":
        pairs.append((f"{r.truth_lat:.5f}", f"{lat:.5f}"))
        pairs.append((f"{abs(r.truth_lon):.5f}", f"{abs(lon):.5f}"))
    # w3w_typo rows keep their garbled address: it never pointed at the true spot anyway.
    swaps.append(pairs)

def apply(text, pairs):
    for old, new in pairs:
        assert old in text, (old, text)
        text = text.replace(old, new)
    return text

t["firefighter_narrative"] = [apply(x, p) for x, p in zip(t.firefighter_narrative, swaps)]
g["firefighter_narrative"] = [apply(x, p) for x, p in zip(g.firefighter_narrative, swaps)]
if "firefighter_narrative_template" in g:
    g["firefighter_narrative_template"] = t.firefighter_narrative
for df in (t, g):
    df["truth_lat"], df["truth_lon"] = new_lat, new_lon
    df["river_section"] = df.landmark_cross_street.map(dict(zip(sites.site_name, sites.river_section)))
# 4. gold for w3w/gps rows depends on where the point is
g["gold_site"] = [gold_for(r) for r in g.itertuples()]

w3w_client.save_cache(cache)
t.to_csv("incidents_with_narratives.csv", index=False)
g.to_csv("incidents_with_narratives_llm.csv", index=False)
changed = sum(bool(p) for p in swaps)
print(f"re-placed {len(t)} incidents; swapped coordinates/addresses in {changed} notes")

"""
Step 1 - add a synthetic free-text firefighter narrative to each incident.

Input : historical_incidents_clean.csv  (Divya's synthetic incidents + real discharge)
Output: incidents_with_narratives.csv

Each narrative mentions the incident location in ONE of these ways, so the
extractor in step 2 has to deal with the same mess a real CAD/NFIRS narrative has:

  w3w       ///three.word.address                      (exact, ~3 m)
  w3w_typo  a what3words address with one word garbled (tests the known weak spot)
  gps       decimal lat/long in a couple of formats    (exact)
  landmark  a named river site, sometimes misspelled   (site centroid, 100s of m)
  vague     "north bank, below the rocks"              (not placeable)
  none      no location at all                         (not placeable)

The true coordinates are kept in truth_lat / truth_lon so step 2 can measure how
far off each method is. That distance is the one honest result a synthetic
dataset can give you: the incidents are fake, but the location-format problem isn't.

Format mix (edit FORMAT_MIX below) is an ASSUMPTION, not data. State it in the report.
"""
import random

import pandas as pd

import river_geometry as rg
import w3w_client

SEED = 42
FORMAT_MIX = {  # share of narratives using each location format
    "landmark": 0.45,
    "w3w": 0.15,
    "w3w_typo": 0.02,
    "gps": 0.10,
    "vague": 0.15,
    "none": 0.13,
}

rng = random.Random(SEED)
place_rng = random.Random(SEED + 1)  # separate stream so placement changes don't reshuffle the notes

# Where simulated incidents happen (ASSUMPTIONS - state them in the report):
SPREAD_SHARE = 0.35   # share of incidents anywhere along the river, not at a named site
SITE_SPREAD_M = 150   # typical distance along the river from the site for the rest
BANK_OFFSET_M = 50    # max sideways offset from the centerline (river is ~150-400 m wide)

sites = pd.read_csv("river_sites.csv")
site_by_name = {r.site_name: r for r in sites.itertuples()}

VAGUE_PHRASES = [
    "north bank, below the rocks",
    "south shore, just downstream of the island",
    "mid-river on a rock ledge",
    "trail side, near the footbridge",
    "river left, past the big bend",
    "on the rocks by the old pilings",
]

OPENERS = [
    "{unit} on scene.",
    "{unit} arrived.",
    "Dispatched for water rescue, {unit} first due.",
    "{unit} responded with boat.",
]

SITUATIONS = {
    "working_rescue": [
        "{n} {who} in the water, unable to self-rescue.",
        "{n} {who} stranded on rock, rising water.",
        "{who} pinned against strainer, {n} pt(s).",
    ],
    "fatality_recovery": [
        "Recovery operation, 1 pt submerged.",
        "Body recovery, pt located by dive team.",
    ],
    "assist_no_rescue": [
        "{n} {who} requesting assistance, able to walk out with escort.",
        "{who} overturned, {n} pt(s) reached shore on own.",
    ],
    "medical_eval": [
        "{n} pt(s) out of water, evaluated for cold exposure.",
        "Pt with minor injury from fall on rocks, EMS eval.",
    ],
    "good_intent_cancelled": [
        "Caller reported {who} in distress, all parties located OK.",
        "No pts found, cancelled after search of area.",
    ],
}

CLOSERS = [
    "Cleared.",
    "Returned to service.",
    "Handed off to EMS.",
    "Command terminated.",
    "",
]


def misspell(s):
    """Drop, double, or swap one letter - roughly how names get typed in the field."""
    if len(s) < 5:
        return s
    i = rng.randrange(1, len(s) - 2)
    op = rng.choice(["drop", "double", "swap"])
    if op == "drop":
        return s[:i] + s[i + 1:]
    if op == "double":
        return s[:i] + s[i] + s[i:]
    return s[:i] + s[i + 1] + s[i] + s[i + 2:]


def location_phrase(fmt, row, cache):
    lat, lon = row.latitude, row.longitude
    if fmt in ("w3w", "w3w_typo"):
        words = w3w_client.coords_to_words(lat, lon, cache, rng)
        if fmt == "w3w_typo":
            parts = words.split(".")
            k = rng.randrange(3)
            garbled = misspell(parts[k]) if rng.random() < 0.5 else parts[k]
            parts[k] = garbled if garbled != parts[k] else parts[k] + "s"
            words = ".".join(parts)
            # Mock mode only: imitate real what3words, where a garbled word is often
            # still a valid address - just somewhere else entirely. Live mode needs no
            # simulation; the API returns wherever the typo really points.
            if not w3w_client.api_key() and words not in cache and rng.random() < 0.7:
                cache[words] = {"lat": round(lat + rng.uniform(-3, 3), 6),
                                "lng": round(lon + rng.uniform(-3, 3), 6),
                                "mock": True, "simulated_typo": True}
        return rng.choice(["Loc ///{w}.", "Pt located at ///{w}.", "w3w ///{w}"]).format(w=words)
    if fmt == "gps":
        style = rng.choice(["dec", "dec_space", "nw"])
        if style == "dec":
            return f"GPS {lat:.5f},{lon:.5f}."
        if style == "dec_space":
            return f"Coords {lat:.5f}, {lon:.5f}."
        return f"N{lat:.5f} W{abs(lon):.5f}."
    if fmt == "landmark":
        site = site_by_name[row.landmark_cross_street]
        name = rng.choice(site.aliases.split("|"))
        if rng.random() < 0.20:
            name = misspell(name)
        if rng.random() < 0.5:
            name = name.title()
        return rng.choice(["At {s}.", "Near {s}.", "Loc {s}.", "{s} area."]).format(s=name)
    if fmt == "vague":
        return rng.choice(VAGUE_PHRASES).capitalize() + "."
    return ""


def who(row):
    craft = str(row.watercraft_involved) if pd.notna(row.watercraft_involved) else ""
    if craft and craft.lower() not in ("none", "nan", "unknown"):
        return craft.lower().split("/")[0] + " party"
    return rng.choice(["swimmer(s)", "wader(s)", "hiker(s)", "person(s)"])


def build_narrative(row, fmt, cache):
    stn = str(row.fire_department_station).replace("RFD-", "")
    unit = "Boat 1" if stn == "RiverOps" else f"E{stn}"
    n = int(row.people_involved) if pd.notna(row.people_involved) else 1
    situation = rng.choice(SITUATIONS.get(row.incident_class, ["Water incident."]))
    parts = [
        rng.choice(OPENERS).format(unit=unit),
        situation.format(n=n, who=who(row)),
        location_phrase(fmt, row, cache),
        rng.choice(CLOSERS),
    ]
    # Location isn't always in the same slot in a real narrative.
    if parts[2] and rng.random() < 0.4:
        parts[1], parts[2] = parts[2], parts[1]
    return " ".join(p for p in parts if p)


def place_on_river(df):
    """Replace Divya's points (scattered around site centres, often on land) with points on
    the water: most near their site, SPREAD_SHARE anywhere along the river. An incident that
    lands between sites is attributed to the nearest site, which is the name a firefighter
    would most likely write."""
    site_ch = {r.site_name: rg.project(r.latitude, r.longitude)[0] for r in sites.itertuples()}
    lo, hi = min(site_ch.values()) - 300, max(site_ch.values()) + 300
    lats, lons, names, secs, spread = [], [], [], [], []
    for r in df.itertuples():
        if place_rng.random() < SPREAD_SHARE:
            ch = place_rng.uniform(lo, hi)
            name = min(site_ch, key=lambda n: abs(site_ch[n] - ch))
            spread.append(True)
        else:
            name = r.landmark_cross_street
            ch = site_ch[name] + place_rng.gauss(0, SITE_SPREAD_M)
            spread.append(False)
        lat, lon = rg.point_at(ch, place_rng.uniform(-BANK_OFFSET_M, BANK_OFFSET_M))
        lats.append(round(lat, 6)); lons.append(round(lon, 6)); names.append(name)
        secs.append(site_by_name[name].river_section)
    df["latitude"], df["longitude"] = lats, lons
    df["landmark_cross_street"], df["river_section"] = names, secs
    df["between_sites_sim"] = spread


def main():
    df = pd.read_csv("historical_incidents_clean.csv")
    cache = w3w_client.load_cache()
    formats = list(FORMAT_MIX)
    weights = list(FORMAT_MIX.values())

    place_on_river(df)
    df["location_format_true"] = [rng.choices(formats, weights)[0] for _ in range(len(df))]
    df["firefighter_narrative"] = [
        build_narrative(r, f, cache) for r, f in zip(df.itertuples(), df.location_format_true)
    ]
    df = df.rename(columns={"latitude": "truth_lat", "longitude": "truth_lon"})
    # Divya's placeholder w3w strings were never real addresses; drop to avoid confusion.
    df = df.drop(columns=["what3words_synthetic"], errors="ignore")

    w3w_client.save_cache(cache)
    df.to_csv("incidents_with_narratives.csv", index=False)
    mode = "LIVE API" if w3w_client.api_key() else "MOCK (no API key)"
    print(f"Wrote {len(df)} narratives. what3words mode: {mode}")
    print(df.location_format_true.value_counts().to_string())


if __name__ == "__main__":
    main()

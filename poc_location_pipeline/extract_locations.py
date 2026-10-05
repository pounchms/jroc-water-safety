"""
Step 2 - pull a map point out of each free-text narrative.

Input : incidents_with_narratives.csv, river_sites.csv, w3w_cache.json
Output: incidents_geocoded.csv   (load this into Tableau)
        location_match_summary.csv

Order of preference (most precise first):
  1. what3words  ///word.word.word  -> cache, then live API if W3W_API_KEY is set
  2. GPS         decimal lat/long (with or without N/W)
  3. landmark    exact alias match, then fuzzy match (catches typos) against river_sites.csv
  4. unplaced

The extractor never looks at truth_lat/truth_lon or location_format_true. Those
columns are only used afterwards to score it.
"""
import difflib
import math
import re

import pandas as pd

import river_geometry as rg
import w3w_client

W3W_RE = re.compile(r"///\s*([a-z]+\.[a-z]+\.[a-z]+)", re.I)
GPS_RE = re.compile(
    r"(?:N\s*)?(3[6-8]\.\d{3,})\s*,?\s*(?:W\s*)?(-?7[6-8]\.\d{3,})", re.I
)
FUZZY_CUTOFF = 0.84  # lower = catches more typos, but risks false matches
# A real what3words typo usually resolves to a VALID address somewhere else.
# Reject any exact-coordinate result farther than this from the river centerline.
MAX_M_FROM_RIVER = 500


def load_gazetteer():
    sites = pd.read_csv("river_sites.csv")
    alias_to_site = {}
    for r in sites.itertuples():
        for a in [r.site_name.lower(), *r.aliases.split("|")]:
            alias_to_site[a.strip().lower()] = r
    return alias_to_site


def ngrams(text, max_n=4):
    toks = re.findall(r"[a-z0-9'\-]+", text.lower())
    for n in range(max_n, 0, -1):
        for i in range(len(toks) - n + 1):
            yield " ".join(toks[i:i + n])


def match_landmark(text, alias_to_site):
    low = text.lower()
    # Exact alias hit - prefer the longest alias so "pony pasture rapids" beats "pony pasture".
    for alias in sorted(alias_to_site, key=len, reverse=True):
        if re.search(rf"(?<![a-z]){re.escape(alias)}(?![a-z])", low):
            return alias_to_site[alias], "landmark_exact", 1.0
    # Fuzzy: compare every 1-4 word window against every alias.
    best = (None, 0.0)
    aliases = list(alias_to_site)
    for gram in ngrams(text):
        if len(gram) < 5:
            continue
        hit = difflib.get_close_matches(gram, aliases, n=1, cutoff=FUZZY_CUTOFF)
        if hit:
            score = difflib.SequenceMatcher(None, gram, hit[0]).ratio()
            if score > best[1]:
                best = (alias_to_site[hit[0]], score)
    if best[0] is not None:
        return best[0], "landmark_fuzzy", round(best[1], 3)
    return None


def near_river(lat, lon, alias_to_site=None):
    # alias_to_site kept for backward compatibility; the check now uses the river itself.
    return rg.distance_to_river_m(lat, lon) <= MAX_M_FROM_RIVER


def extract(text, alias_to_site, cache):
    """Return dict with lat, lon, method, detail, confidence."""
    text = text if isinstance(text, str) else ""

    m = W3W_RE.search(text)
    if m:
        res = w3w_client.words_to_coords(m.group(1), cache)
        if res and not near_river(res[0], res[1], alias_to_site):
            return dict(lat=None, lon=None, method="w3w_off_river",
                        detail=f"///{m.group(1).lower()} -> {res[0]:.4f},{res[1]:.4f}", confidence=0.0)
        if res:
            return dict(lat=res[0], lon=res[1], method="what3words",
                        detail=f"///{m.group(1).lower()} ({res[2]})", confidence=1.0)
        # Found an address that doesn't resolve - flag it rather than silently guessing.
        fallback = match_landmark(text.replace(m.group(0), ""), alias_to_site)
        if fallback:
            site, how, conf = fallback
            return dict(lat=site.latitude, lon=site.longitude, method=how,
                        detail=f"{site.site_name}; w3w ///{m.group(1)} unresolved", confidence=conf)
        return dict(lat=None, lon=None, method="w3w_unresolved",
                    detail=f"///{m.group(1).lower()}", confidence=0.0)

    m = GPS_RE.search(text)
    if m:
        lat, lon = float(m.group(1)), float(m.group(2))
        lon = -abs(lon)  # Richmond is west of Greenwich; "W77.5" means -77.5
        if not near_river(lat, lon, alias_to_site):
            return dict(lat=None, lon=None, method="gps_off_river", detail=m.group(0), confidence=0.0)
        return dict(lat=lat, lon=lon, method="gps", detail=m.group(0), confidence=1.0)

    hit = match_landmark(text, alias_to_site)
    if hit:
        site, how, conf = hit
        return dict(lat=site.latitude, lon=site.longitude, method=how,
                    detail=site.site_name, confidence=conf)

    return dict(lat=None, lon=None, method="unplaced", detail="", confidence=0.0)


def haversine_m(lat1, lon1, lat2, lon2):
    if any(pd.isna(v) for v in (lat1, lon1, lat2, lon2)):
        return None
    r = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return round(2 * r * math.asin(math.sqrt(a)), 1)


def main():
    """Default (since 2026-09-28): Claude-written notes + the LLM parser's placements, with the
    rule parser kept alongside in rules_* columns. `python extract_locations.py --rules` rebuilds
    the old way (template notes, rule parser) if the LLM files are missing or for comparison."""
    from pathlib import Path
    alias_to_site = load_gazetteer()
    cache = w3w_client.load_cache()
    use_llm = ("--rules" not in __import__("sys").argv and Path("incidents_with_narratives_llm.csv").exists()
               and Path("llm_mainllm_results.csv").exists())
    df = pd.read_csv("incidents_with_narratives_llm.csv" if use_llm else "incidents_with_narratives.csv")

    rules = pd.DataFrame([extract(t, alias_to_site, cache) for t in df.firefighter_narrative])
    rules.columns = ["map_lat", "map_lon", "location_method", "location_detail", "location_confidence"]
    if use_llm:
        llm = pd.read_csv("llm_mainllm_results.csv")
        assert len(llm) == len(df), "llm_mainllm_results.csv is stale - rerun: python llm_extract.py mainllm"
        import hashlib
        cur = [hashlib.sha1(str(n).encode()).hexdigest()[:12] for n in df.firefighter_narrative]
        assert "note_sha" in llm and list(llm.note_sha) == cur, \
            "notes changed since the LLM parser ran - rerun: python llm_extract.py mainllm"
        conf = {"high": 1.0, "medium": 0.6, "low": 0.3}
        out = pd.DataFrame({
            "map_lat": llm.map_lat, "map_lon": llm.map_lon, "location_method": llm.location_method,
            "location_detail": llm.llm_site_id.fillna(llm.llm_other_place).fillna(""),
            "location_confidence": llm.llm_confidence.map(conf).fillna(0.0)})
        out.loc[out.map_lat.isna(), "location_confidence"] = 0.0
        df = pd.concat([df, out, rules.add_prefix("rules_")[["rules_map_lat", "rules_map_lon",
                                                              "rules_location_method"]]], axis=1)
        df["parser"] = llm["llm_model"].iloc[0] if "llm_model" in llm else "llm"
    else:
        df = pd.concat([df, rules], axis=1)
        df["parser"] = "rules"
    df["placed"] = df.map_lat.notna()
    df["error_m"] = [haversine_m(a, b, c, d) for a, b, c, d in
                     zip(df.map_lat, df.map_lon, df.truth_lat, df.truth_lon)]
    w3w_client.save_cache(cache)

    df.to_csv("incidents_geocoded.csv", index=False)  # INTERNAL - do not share outside the team

    # what3words API licence 6.3(b): no showing a 3-word address next to its coordinates
    # to third parties. The dashboard file therefore drops the words, keeps the point.
    public = df.copy()
    for col in ("firefighter_narrative", "location_detail"):
        public[col] = public[col].fillna("").str.replace(W3W_RE, "///[w3w]", regex=True)
    public["location_detail"] = public["location_detail"].str.replace(
        r"-> [-\d.,]+", "", regex=True)
    # The dashboard file should look like RFD data + our parse, nothing more. Drop every
    # column that only exists because the incidents are simulated (the "answer key"),
    # so no chart can accidentally plot or group by information RFD wouldn't have.
    sites = pd.read_csv("river_sites.csv")
    def nearest_site(lat, lon):
        if pd.isna(lat):
            return None
        return min(sites.itertuples(),
                   key=lambda s: haversine_m(lat, lon, s.latitude, s.longitude)).site_name
    public["parsed_site"] = [nearest_site(a, b) for a, b in zip(public.map_lat, public.map_lon)]
    answer_key = ["truth_lat", "truth_lon", "location_format_true", "landmark_cross_street",
                  "river_section", "location_quality", "between_sites_sim",
                  # generator-side answer key / provenance for the Claude-written notes:
                  "gold_site", "decoy_site", "style_variant", "gen_ok", "gen_model",
                  "firefighter_narrative_template"]
    public = public.drop(columns=answer_key, errors="ignore")
    public = public.rename(columns={"error_m": "sim_error_m"})
    # Map design: place-name hits all sit on a site centre, so Tableau draws them as one
    # sized circle per site; what3words/GPS hits are real points, drawn as individual dots.
    public["map_layer"] = public.location_method.map(
        {"landmark_exact": "site_circle", "landmark_fuzzy": "site_circle", "llm_site": "site_circle",
         "what3words": "precise_point", "gps": "precise_point",
         "llm_what3words": "precise_point", "llm_gps": "precise_point"}).fillna("not_mapped")
    # Map definition: only calls where crews reached a person. Cancelled-before-contact calls come
    # off the map via the structured incident type, not the note. Filter the dashboard on on_map.
    public["on_map"] = public.map_lat.notna() & (public.incident_class != "good_intent_cancelled")
    public.loc[~public.on_map, "map_layer"] = "not_mapped"
    public.to_csv("incidents_public.csv", index=False)

    summary = (df.groupby("location_method")
                 .agg(incidents=("incident_id", "size"),
                      median_error_m=("error_m", "median"),
                      p90_error_m=("error_m", lambda s: s.quantile(0.9)),
                      max_error_m=("error_m", "max"))
                 .sort_values("incidents", ascending=False))
    summary["share"] = (summary.incidents / len(df)).round(3)
    summary.to_csv("location_match_summary.csv")

    print(summary.to_string())
    print(f"\nPlaced on map: {df.placed.sum()} of {len(df)} ({df.placed.mean():.0%})")
    print("\nTrue format vs. what the extractor did:")
    print(pd.crosstab(df.location_format_true, df.location_method).to_string())


if __name__ == "__main__":
    main()

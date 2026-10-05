"""
Step 1b - rewrite every synthetic narrative with Claude, so the parsers are tested
on phrasing they have never seen.

Why this exists: generate_narratives.py builds notes from templates using the same
site nicknames the rule parser matches against, so the rule parser scores 1.00 on
them. That number measures nothing. Here the generator is told only the facts of the
incident and the CANONICAL site name - never the alias list in river_sites.csv - and
writes the note the way a firefighter would. The parsers have to cope with whatever
comes out.

What stays fixed (copied from incidents_with_narratives.csv, so the two files are a
clean A/B comparison): the incident, its true point on the river, its location format,
and the exact ///what3words or GPS string. Only the wording changes.

The answer key is written by THIS script, never by the model:
  gold_site    - where the patient was (same rule as evaluate_parsers.py main set)
  decoy_site   - a second river site the note mentions that is NOT the answer
  style_variant- which complication the note was asked to contain
If the model drops a required ///address or GPS string, the row is retried once and
then flagged (gen_ok = False) rather than silently kept.

STYLE_MIX is an ASSUMPTION about how messy real notes are, not data. Say so in the report.

Usage (Windows cmd, in this folder):
  set ANTHROPIC_API_KEY=sk-ant-...
  python generate_narratives_llm.py --dry-run      # print 3 prompts, no API calls
  python generate_narratives_llm.py --limit 100    # first 100 incidents (~$0.20)
  python generate_narratives_llm.py                # all 525
Free route (notes written in a Claude chat session, no API key):
  python generate_narratives_llm.py --export gen_prompts.jsonl
  ... chat session writes gen_answers.jsonl ...
  set ANTHROPIC_GEN_MODEL=claude-chat-session
  python generate_narratives_llm.py --import gen_answers.jsonl
Output: incidents_with_narratives_llm.csv. Every answer is cached in
gen_cache.json (key = model + prompt version + incident id), so reruns are free.
"""
import hashlib
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd

from extract_locations import GPS_RE, W3W_RE, haversine_m

MODEL = os.environ.get("ANTHROPIC_GEN_MODEL", os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5"))
PROMPT_VERSION = "g1"
SEED = 7
API_URL = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/") + "/v1/messages"
HERE = Path(__file__).parent
CACHE_PATH = HERE / "gen_cache.json"
IN_PATH, OUT_PATH = HERE / "incidents_with_narratives.csv", HERE / "incidents_with_narratives_llm.csv"

STYLE_MIX = {            # share of notes given each complication (ASSUMPTION)
    "plain": 0.45,
    "dispatch_mismatch": 0.15,   # dispatched/caller said site A, patient found at the real location
    "residence": 0.10,           # patient's home street is mentioned
    "relative": 0.15,            # "200 yds below X" instead of "at X" (place-name notes only)
    "shorthand": 0.15,           # heavy abbreviation, run-on, minimal punctuation
}

SITES = pd.read_csv(HERE / "river_sites.csv")
NAME_TO_ID = dict(zip(SITES.site_name, SITES.site_id))
ID_TO_NAME = dict(zip(SITES.site_id, SITES.site_name))
rng = random.Random(SEED)


def nearest_site(lat, lon):
    return min(SITES.itertuples(), key=lambda s: haversine_m(lat, lon, s.latitude, s.longitude)).site_id


def gold_for(row):
    """Identical rule to evaluate_parsers.py so scores on both files are comparable."""
    if row.location_format_true == "landmark":
        return NAME_TO_ID[row.landmark_cross_street]
    if row.location_format_true in ("w3w", "gps"):
        return nearest_site(row.truth_lat, row.truth_lon)
    return "NONE"  # vague, none, and what3words typos (correct answer: don't place it)


SYSTEM = """You write short incident narratives the way a Richmond Fire Department officer types them into the report system after a water call on the James River: terse, past tense, unit designators, abbreviations (pt, PFD, EMS, RTS), no full sentences required, 1-4 lines. No names of people. No headings, no quotation marks.

You will get the facts of one incident and an instruction about how the LOCATION must appear. Follow the location instruction exactly:
- If it gives a what3words address or GPS coordinates, copy them character for character.
- If it gives a river site, refer to it the way a local firefighter plausibly would - full name, a shortening, a nickname, a nearby road or access point, or a small misspelling. Vary it. Do not always use the full name.
- Do not mention any other river location, park, rapid, bridge, island or street unless the instruction tells you to.
Always call the write_narrative tool."""

TOOL = {
    "name": "write_narrative",
    "description": "Return the narrative text only.",
    "input_schema": {"type": "object", "properties": {"narrative": {"type": "string"}},
                     "required": ["narrative"]},
}


def location_instruction(row, fmt, variant, decoy):
    tmpl = row.firefighter_narrative
    site = row.landmark_cross_street
    if fmt in ("w3w", "w3w_typo"):
        loc = "what3words address ///" + W3W_RE.search(tmpl).group(1)
    elif fmt == "gps":
        loc = "GPS coordinates " + GPS_RE.search(tmpl).group(0)
    elif fmt == "landmark":
        loc = f"the river site '{site}'"
    elif fmt == "vague":
        # g2: the g1 example "downstream of the island" means Belle Isle to any Richmond reader,
        # so it wasn't vague. Example list now contains nothing a local could resolve to a site.
        return ("Describe where the patient was only vaguely (a bank, a rock, a gravel bar, mid-channel, "
                "river left/right) with NO named place, road, site, island or coordinates, and nothing a "
                "local could use to identify a specific site.")
    else:
        return "Do not say where the incident happened at all."

    base = f"The patient was located at {loc}."
    if variant == "dispatch_mismatch":
        return (base + f" The call was originally dispatched to (or the caller reported) '{decoy}', "
                "but that turned out to be wrong. Mention both, and make it clear from context where the "
                "patient actually was - without using the words 'actual location'.")
    if variant == "residence":
        return (base + " Also mention the patient's home address as a Richmond street address that is NOT "
                "near the river (e.g. a Fan, Museum District or Northside street). Name no other river place.")
    if variant == "relative" and fmt == "landmark":
        return (f"The patient was a short distance (under 300 yards) upstream, downstream, or across from "
                f"the river site '{site}'. Say it that way (e.g. 'approx 150 yds below ...').")
    if variant == "shorthand":
        return base + " Write in heavy shorthand: run-on, minimal punctuation, lots of abbreviations."
    return base


def facts(row):
    hour = pd.to_datetime(row.alarm_datetime).hour
    stn = str(row.fire_department_station).replace("RFD-", "")
    unit = "Boat 1" if stn == "RiverOps" else f"Engine {stn}"
    craft = row.watercraft_involved if pd.notna(row.watercraft_involved) else "none"
    return (f"Unit: {unit}. Time: {hour:02d}:00. Call type: {row.incident_class.replace('_', ' ')}. "
            f"People involved: {row.people_involved}. Watercraft: {craft}. Activity: {row.activity_type}. "
            f"Actions taken: {row.actions_taken}. Outcome: {row.casualty_outcome.replace('_', ' ')}. "
            f"Factors: {row.human_factors if pd.notna(row.human_factors) else 'none noted'}.")


def required_string(row, fmt):
    """Text that MUST appear verbatim in the output for the answer key to stay valid."""
    if fmt in ("w3w", "w3w_typo"):
        return W3W_RE.search(row.firefighter_narrative).group(1).lower()
    if fmt == "gps":
        m = GPS_RE.search(row.firefighter_narrative)
        return m.group(1)  # latitude digits are enough to prove the coords were copied
    return None


def call_claude(user, usage):
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("Set ANTHROPIC_API_KEY first (see the top of this file).")
    body = {"model": MODEL, "max_tokens": 300, "temperature": 1.0,
            "system": [{"type": "text", "text": SYSTEM, "cache_control": {"type": "ephemeral"}}],
            "tools": [TOOL], "tool_choice": {"type": "tool", "name": "write_narrative"},
            "messages": [{"role": "user", "content": user}]}
    for attempt in range(5):
        req = urllib.request.Request(API_URL, data=json.dumps(body).encode(), headers={
            "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                data = json.loads(r.read())
            break
        except urllib.error.HTTPError as e:
            msg = e.read().decode(errors="replace")
            if e.code == 400 and "temperature" in msg and "temperature" in body:
                body.pop("temperature")
                continue
            if e.code in (429, 500, 529) and attempt < 4:
                time.sleep(2 ** attempt)
                continue
            sys.exit(f"API error {e.code}: {msg}")
    u = data.get("usage", {})
    usage["in"] += u.get("input_tokens", 0) + u.get("cache_read_input_tokens", 0) + u.get("cache_creation_input_tokens", 0)
    usage["out"] += u.get("output_tokens", 0)
    return next(b["input"]["narrative"] for b in data["content"] if b["type"] == "tool_use").strip()


def main():
    dry = "--dry-run" in sys.argv
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
    df = pd.read_csv(IN_PATH)
    # Draw variants/decoys for ALL rows before slicing, so --limit 100 and a full run agree on row 1-100.
    variants, decoys = [], []
    for r in df.itertuples():
        v = rng.choices(list(STYLE_MIX), list(STYLE_MIX.values()))[0]
        if v == "relative" and r.location_format_true != "landmark":
            v = "plain"
        if v in ("dispatch_mismatch", "residence") and r.location_format_true in ("vague", "none"):
            v = "plain"
        g = gold_for(r)
        d = rng.choice([s for s in SITES.site_id if s != g]) if v == "dispatch_mismatch" else None
        variants.append(v)
        decoys.append(d)
    df["style_variant"], df["decoy_site"] = variants, decoys
    df["gold_site"] = [gold_for(r) for r in df.itertuples()]
    if limit:
        df = df.head(limit).copy()

    # --export / --import: write the notes in a Claude chat session instead of via the API (free).
    # Export writes one prompt per line; the chat returns {"incident_id", "narrative"} lines;
    # import loads them into the same cache the API path uses, then the normal run below applies
    # the same checks (verbatim address/GPS, gen_ok flag) and writes the same output file.
    if "--export" in sys.argv:
        path = HERE / sys.argv[sys.argv.index("--export") + 1]
        with open(path, "w", encoding="utf-8") as f:
            only = sys.argv[sys.argv.index("--only") + 1] if "--only" in sys.argv else None
            for r in df.itertuples():
                if only and r.location_format_true != only:
                    continue
                user = f"{facts(r)}\n\nLOCATION: {location_instruction(r, r.location_format_true, r.style_variant, ID_TO_NAME.get(r.decoy_site))}"
                f.write(json.dumps({"incident_id": r.incident_id, "system": SYSTEM, "user": user}) + "\n")
        print(f"Exported prompts to {path.name}")
        return
    cache = json.loads(CACHE_PATH.read_text()) if CACHE_PATH.exists() else {}
    if "--import" in sys.argv:
        path = HERE / sys.argv[sys.argv.index("--import") + 1]
        n = 0
        for line in open(path, encoding="utf-8"):
            if line.strip():
                a = json.loads(line)
                cache[hashlib.sha256(f"{MODEL}|{PROMPT_VERSION}|{a['incident_id']}".encode()).hexdigest()] = a["narrative"].strip()
                n += 1
        print(f"Imported {n} narratives from {path.name} as model '{MODEL}'")
    usage, out, ok = {"in": 0, "out": 0}, [], []
    for i, r in enumerate(df.itertuples()):
        fmt = r.location_format_true
        decoy_name = ID_TO_NAME.get(r.decoy_site)
        user = f"{facts(r)}\n\nLOCATION: {location_instruction(r, fmt, r.style_variant, decoy_name)}"
        if dry:
            if i < 3:
                print(f"--- {r.incident_id} [{fmt} / {r.style_variant}] gold={r.gold_site}\n{user}\n")
            continue
        need = required_string(r, fmt)
        k = hashlib.sha256(f"{MODEL}|{PROMPT_VERSION}|{r.incident_id}".encode()).hexdigest()
        good = True
        if k not in cache:
            text = call_claude(user, usage)
            if need and need not in text.lower():
                text = call_claude(user + "\n\nYou MUST include the address/coordinates exactly as given.", usage)
            cache[k] = text
            if i % 20 == 0:
                CACHE_PATH.write_text(json.dumps(cache, indent=1))
        text = cache[k]
        if need and need not in text.lower():
            good = False
        out.append(text)
        ok.append(good)
    if dry:
        print(df.style_variant.value_counts().to_string())
        return
    CACHE_PATH.write_text(json.dumps(cache, indent=1))
    df["firefighter_narrative_template"] = df.firefighter_narrative
    df["firefighter_narrative"] = out
    df["gen_ok"] = ok
    df["gen_model"] = MODEL
    df.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(df)} LLM narratives to {OUT_PATH.name} ({MODEL}); "
          f"{len(df) - sum(ok)} flagged gen_ok=False. New tokens: {usage['in']} in / {usage['out']} out.")
    print(df.style_variant.value_counts().to_string())


if __name__ == "__main__":
    main()

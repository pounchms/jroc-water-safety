"""
LLM location extractor (Claude) - the "rules -> LLM -> rules" hybrid.

The LLM only READS the note. It never produces coordinates. It must answer through
a fixed tool schema:
  * incident_site_id : one of the ids in river_sites.csv, or null
  * other_place      : a named place that is NOT in the list (e.g. "Texas Beach"), or null
  * what3words / gps : copied verbatim from the note if present, or null
  * evidence         : the exact words in the note that the answer is based on
Coordinates then come from the same deterministic code as the rule parser
(site centroid, what3words lookup, 500 m off-river check), so the model cannot
invent a plausible-looking point on the James.

Usage (Windows cmd, in this folder):
  set ANTHROPIC_API_KEY=sk-ant-...
  python llm_extract.py hard            # 38 hand-written hard notes  -> llm_hard_results.csv
  python llm_extract.py main --limit 100  # first 100 simulated notes -> llm_main_results.csv
  python llm_extract.py main            # all 525
  python llm_extract.py mainllm         # Claude-written notes (generate_narratives_llm.py)

Every API answer is cached in llm_cache.json (key = model + prompt version + note),
so reruns are free and results are reproducible. Delete the cache to re-query.
Never commit your API key; it is read only from the environment.
"""
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd

import w3w_client
from extract_locations import GPS_RE, load_gazetteer, near_river

# LLM_BACKEND=ollama runs a local model on your own GPU (free, nothing leaves the machine).
# Default is the Anthropic API. See "Local model" in README.md.
BACKEND = os.environ.get("LLM_BACKEND", "anthropic").lower()
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/") + "/api/chat"
MODEL = (os.environ.get("OLLAMA_MODEL", "qwen2.5:14b") if BACKEND == "ollama"
         else os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5"))
PROMPT_VERSION = "v1"
API_URL = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/") + "/v1/messages"
CACHE_PATH = Path(__file__).with_name("llm_cache.json")
# $ per million tokens (input, output) - update if you change MODEL.
PRICE = {"claude-sonnet-5": (2.0, 10.0), "claude-haiku-4-5": (1.0, 5.0),
         "claude-opus-5-5": (4.0, 20.0)}

SITES = pd.read_csv(Path(__file__).with_name("river_sites.csv"))
SITE_BY_ID = {r.site_id: r for r in SITES.itertuples()}
ALIASES = load_gazetteer()


def system_prompt():
    rows = "\n".join(f"- {r.site_id}: {r.site_name} (also called: {r.aliases.replace('|', ', ')})"
                     for r in SITES.itertuples())
    return f"""You read short fire-department incident notes about water rescues on the James River in Richmond, VA, and identify WHERE THE PATIENT WAS when crews reached them.

Known river sites:
{rows}

Rules:
- Answer only from the note. Never guess a location the note does not state.
- If the note mentions several places (staging area, where the caller was, where the patient lives, a place that was ruled out), choose the place where the patient was actually located or rescued.
- Map abbreviations, nicknames and misspellings to the known site they clearly refer to.
- If the patient's location is a named place that is NOT in the list, set incident_site_id to null and put the name in other_place.
- If the note gives no usable patient location, set incident_site_id and other_place to null.
- Copy any ///three.word.address or GPS coordinates exactly as written.
- Always call the record_location tool."""


TOOL = {
    "name": "record_location",
    "description": "Record where the patient was located, based only on the note.",
    "input_schema": {
        "type": "object",
        "properties": {
            "incident_site_id": {"type": ["string", "null"],
                                 "enum": [*SITES.site_id.tolist(), None]},
            "other_place": {"type": ["string", "null"]},
            "what3words": {"type": ["string", "null"]},
            "gps": {"type": ["string", "null"]},
            "evidence": {"type": "string",
                         "description": "Exact words from the note the answer relies on, or '' if none."},
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        },
        "required": ["incident_site_id", "other_place", "what3words", "gps", "evidence", "confidence"],
    },
}


def load_cache():
    return json.loads(CACHE_PATH.read_text()) if CACHE_PATH.exists() else {}


def save_cache(c):
    CACHE_PATH.write_text(json.dumps(c, indent=1))


def cache_key(note):
    return hashlib.sha256(f"{BACKEND}|{MODEL}|{PROMPT_VERSION}|{note}".encode()).hexdigest()


def call_ollama(note, usage):
    """Same prompt and answer fields as the Claude path. Ollama has no tool calling we can force,
    so the answer is constrained with a JSON schema instead (structured outputs, Ollama >= 0.5)."""
    schema = json.loads(json.dumps(TOOL["input_schema"]))
    # small local models handle a plain string enum better than a nullable one
    schema["properties"]["incident_site_id"] = {"type": "string",
                                                "enum": [*SITES.site_id.tolist(), "NONE"]}
    for f in ("other_place", "what3words", "gps"):
        schema["properties"][f] = {"type": "string"}
    body = {"model": MODEL, "stream": False, "format": schema,
            "options": {"temperature": 0, "seed": 0},
            "messages": [{"role": "system", "content": system_prompt() +
                          "\nReply with JSON only. Use \"NONE\" for no site and \"\" for empty text fields."},
                         {"role": "user", "content": f"Note: {note}"}]}
    req = urllib.request.Request(OLLAMA_URL, data=json.dumps(body).encode(),
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            data = json.loads(r.read())
    except urllib.error.URLError as e:
        sys.exit(f"Can't reach Ollama at {OLLAMA_URL} ({e}). Is the Ollama app running? "
                 f"Did you run: ollama pull {MODEL}")
    usage["in"] += data.get("prompt_eval_count", 0)
    usage["out"] += data.get("eval_count", 0)
    ans = json.loads(data["message"]["content"])
    if ans.get("incident_site_id") in ("NONE", "", None):
        ans["incident_site_id"] = None
    for f in ("other_place", "what3words", "gps"):
        ans[f] = ans.get(f) or None
    return ans


def call_claude(note, usage):
    if BACKEND == "ollama":
        return call_ollama(note, usage)
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("Set ANTHROPIC_API_KEY first (see the top of this file).")
    body = {
        "model": MODEL,
        "max_tokens": 400,
        "temperature": 0,
        "system": [{"type": "text", "text": system_prompt(),
                    "cache_control": {"type": "ephemeral"}}],  # site list is identical every call
        "tools": [TOOL],
        "tool_choice": {"type": "tool", "name": "record_location"},
        "messages": [{"role": "user", "content": f"Note: {note}"}],
    }
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
                body.pop("temperature")  # some models reject it; caching keeps runs reproducible anyway
                continue
            if e.code in (429, 500, 529) and attempt < 4:
                time.sleep(2 ** attempt)
                continue
            sys.exit(f"API error {e.code}: {msg}")
    u = data.get("usage", {})
    usage["in"] += u.get("input_tokens", 0) + u.get("cache_read_input_tokens", 0) \
        + u.get("cache_creation_input_tokens", 0)
    usage["out"] += u.get("output_tokens", 0)
    for block in data["content"]:
        if block["type"] == "tool_use":
            return block["input"]
    raise RuntimeError(f"No tool call in response: {data}")


def resolve(ans, w3w_cache):
    """Deterministic step: turn the LLM's reading into a point (or not)."""
    if ans.get("what3words"):
        res = w3w_client.words_to_coords(ans["what3words"], w3w_cache)
        if res and near_river(res[0], res[1], ALIASES):
            return res[0], res[1], "llm_what3words"
    if ans.get("gps"):
        m = GPS_RE.search(ans["gps"])
        if m:
            lat, lon = float(m.group(1)), -abs(float(m.group(2)))
            if near_river(lat, lon, ALIASES):
                return lat, lon, "llm_gps"
    sid = ans.get("incident_site_id")
    if sid in SITE_BY_ID:
        s = SITE_BY_ID[sid]
        return s.latitude, s.longitude, "llm_site"
    if ans.get("other_place"):
        return None, None, "llm_off_list"
    return None, None, "unplaced"


def run(notes, out_path):
    cache, w3w_cache = load_cache(), w3w_client.load_cache()
    usage = {"in": 0, "out": 0}
    rows = []
    for i, note in enumerate(notes):
        k = cache_key(note)
        if k not in cache:
            cache[k] = call_claude(note, usage)
            if i % 20 == 0:
                save_cache(cache)
        ans = cache[k]
        lat, lon, method = resolve(ans, w3w_cache)
        rows.append({"map_lat": lat, "map_lon": lon, "location_method": method,
                     "llm_site_id": ans.get("incident_site_id"),
                     "llm_other_place": ans.get("other_place"),
                     "llm_evidence": ans.get("evidence"),
                     "llm_confidence": ans.get("confidence"),
                     "llm_model": f"{BACKEND}:{MODEL}",
                     "note_sha": hashlib.sha1(str(note).encode()).hexdigest()[:12]})
    save_cache(cache)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    pin, pout = PRICE.get(MODEL, (0, 0))
    cost = usage["in"] / 1e6 * pin + usage["out"] / 1e6 * pout
    print(f"{len(notes)} notes, {MODEL}. New API tokens: {usage['in']} in / {usage['out']} out "
          f"(~${cost:.2f}, before prompt-cache discount). Wrote {out_path}")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "hard"
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
    if which == "hard":
        df = pd.read_csv("hard_test_notes.csv")
        run(df.note.tolist()[:limit], "llm_hard_results.csv")
    elif which == "holdout":  # human-written notes, see HOLDOUT_INSTRUCTIONS.md
        df = pd.read_csv("holdout_notes.csv").dropna(subset=["note"])
        run(df.note.tolist()[:limit], "llm_holdout_results.csv")
    elif which == "mainllm":  # Claude-written notes from generate_narratives_llm.py
        df = pd.read_csv("incidents_with_narratives_llm.csv")
        run(df.firefighter_narrative.tolist()[:limit], "llm_mainllm_results.csv")
    else:
        df = pd.read_csv("incidents_with_narratives.csv")
        run(df.firefighter_narrative.tolist()[:limit], "llm_main_results.csv")

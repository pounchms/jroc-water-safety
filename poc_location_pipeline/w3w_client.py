"""
Minimal what3words client with a local JSON cache.

Two modes:
  * LIVE  - set the W3W_API_KEY environment variable. Lookups hit the real API
            and are cached, so each address costs one request, ever.
            Converting ///words -> coordinates needs a paid plan (Basic, $9.99/mo
            for 1,000 lookups at the time of writing). Check whether your plan
            also covers coordinates -> ///words before running the generator live.
  * MOCK  - no key. The generator invents three-word strings and writes them
            to the cache with "mock": true. The extractor resolves them from the
            cache only. Mock addresses are NOT real what3words addresses - do not
            paste them into the what3words app and expect them to land on the river.

The cache file is the single source of truth for both modes, which keeps the
pipeline runnable with zero API spend and makes a live run reproducible later.
"""
import json
import os
import random
import urllib.parse
import urllib.request
from pathlib import Path

API_BASE = "https://api.what3words.com/v3"
CACHE_PATH = Path(__file__).with_name("w3w_cache.json")

# Small word pool for mock addresses. Real w3w uses ~40k words per language.
_MOCK_WORDS = (
    "river stone paddle heron cedar ripple bank rope canoe maple quiet amber "
    "harbor pebble willow ledger copper lantern meadow falcon hollow crest "
    "timber orbit velvet summit anchor cobalt thistle ember marble prairie "
    "juniper saddle beacon glacier tundra quill basin clover drift canyon "
    "fable garnet hazel island jasper kettle lilac mosaic nectar otter parcel"
).split()


def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def save_cache(cache):
    CACHE_PATH.write_text(json.dumps(cache, indent=1, sort_keys=True))


def api_key():
    return os.environ.get("W3W_API_KEY")


def _get(endpoint, params):
    params = dict(params, key=api_key())
    url = f"{API_BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())


def coords_to_words(lat, lng, cache, rng=random):
    """Return a three-word address for a point; registers it in the cache."""
    if api_key():
        data = _get("convert-to-3wa", {"coordinates": f"{lat},{lng}"})
        words = data["words"]
        c = data["coordinates"]
        cache[words] = {"lat": c["lat"], "lng": c["lng"], "mock": False}
        return words
    while True:
        words = ".".join(rng.sample(_MOCK_WORDS, 3))
        if words not in cache:
            # Mock mode: store the true point (w3w squares are 3m, so the
            # real API would return the square centre - within ~2m of this).
            cache[words] = {"lat": round(lat, 6), "lng": round(lng, 6), "mock": True}
            return words


def words_to_coords(words, cache):
    """Resolve ///words -> (lat, lng, source) or None if it can't be resolved."""
    words = words.lower().strip("/ ")
    if words in cache:
        hit = cache[words]
        return hit["lat"], hit["lng"], "cache_mock" if hit["mock"] else "cache_live"
    if api_key():
        try:
            data = _get("convert-to-coordinates", {"words": words})
            c = data["coordinates"]
            cache[words] = {"lat": c["lat"], "lng": c["lng"], "mock": False}
            return c["lat"], c["lng"], "api"
        except Exception:
            return None
    return None

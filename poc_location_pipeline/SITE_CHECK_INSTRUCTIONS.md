# Checking the 12 river site locations (task V-03)

**Time:** about 30 minutes. **Tools:** Google Maps in a web browser, and Excel.

## Why this matters

When a note names a place, like "Pony Pasture," the map puts the incident on that site's stored point. Right now those points are rough guesses, and several aren't even on the water. Every place-name incident on the map inherits that error. This task replaces the guesses with real points.

## What point to pick

For each site, pick a point **on the water**, at the spot where people actually get into trouble. That's usually the river right off the access point, or the named rapid, dam, or bridge itself.

- **Not** the parking lot or the trailhead.
- **Not** the middle of the river, unless the named feature is there, like a dam face or a bridge span.
- Stay within about 50 m of the shore at the access point, unless the site is a feature out in the river.

## Steps for each of the 12 sites

1. Open `river_sites.csv` in Excel. Each row is a site, with `site_name`, `latitude`, `longitude` and `coords_verified` columns.
2. In Google Maps, search the site name plus "Richmond VA", for example "Pony Pasture Richmond VA". Switch to **Satellite** view and zoom in until you can see the water.
3. **Right-click** the point on the water you've chosen. The first line of the menu is two numbers, like `37.5535, -77.5362`. Click them to copy.
4. Paste the first number into `latitude` and the second into `longitude`. The second number must stay negative.
5. Change `coords_verified` from `no` to `yes`.
6. If you weren't sure about a site, for example because the name matched two places, note which one you picked and why in a message to Matt.

The sites: Pony Pasture, Huguenot Flatwater, Bosher Dam, 42nd Street, Reedy Creek, Belle Isle, Tredegar / Canal Walk, Brown's Island, Mayo Bridge, Z-Dam Area, Pipeline Rapids, Ancarrow's Landing.

## Don't change

- **Any other column,** especially `aliases` and `site_id`.
- **The order of the rows.**
- **The file name.** Save as **CSV UTF-8** with the same name.

## For Matt, afterward

Run only these two:

```
python extract_locations.py
python evaluate_parsers.py
```

**Don't** rerun `generate_narratives.py`. It would move every simulated incident and create new GPS and what3words strings, which would no longer match the Claude-written notes. Expect the place-name error numbers to drop, and a few what3words/GPS answers in the answer key to shift to a neighbouring site. Both changes are fine and should be mentioned in the report.

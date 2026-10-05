# Writing test notes for the location parser

**Who:** Divya, Jon, Paris, about 10 notes each. Matt doesn't write any, because he has already seen where the parser fails.
**Time:** about 30–45 minutes.
**Due:** ________

## Why this matters

We built a program that reads a firefighter's written notes about a river call and works out where on the James the person was. So far, every note it has been tested on was written by an AI. Your notes are the only test no AI wrote, which makes them the most important numbers in the report.

## Rules (these keep the test fair)

1. **Don't use AI** (ChatGPT, Claude, Copilot and so on) to write or edit your notes.
2. **Only open two files:** this one and `holdout_notes.csv`. Don't look at the other files in the `poc_location_pipeline` folder, the existing notes, or any results, before you've submitted your notes.
3. **Don't compare notes with each other** until everyone has submitted.

## What to write

Picture a Richmond Fire officer typing up a water call on the James afterward. The notes are short and use shorthand: "E16 on scene, 2 tubers stranded on rocks...". Write each note however you think they'd write it.

- **Only write calls where crews reached someone**: rescued, assisted, evaluated, or recovered. Skip calls that were cancelled before crews got there.
- **Say where it happened the way a local would.** Use the names, shortcuts and nicknames you'd actually use. Don't look up how anyone else spells a place.
- **Don't use GPS coordinates or what3words addresses.** Place names and descriptions only.
- **Vary your notes.** Aim for roughly this mix:
  - About half ordinary notes that name one place.
  - A few that mention more than one place, for example where it was dispatched, where the caller was, or where the person lives, plus where the person was actually found.
  - A few set somewhere on the river that isn't on the site list below.
  - A couple with no usable location at all.
  - Anything else you think a real officer might write that could trip up a computer.

## How to label each note

After you write a note, fill in `gold_site`, meaning where the person was when crews reached them:

| gold_site | Use when the person was at... |
|---|---|
| PONY | Pony Pasture |
| HUG | Huguenot Flatwater |
| BOSH | Bosher Dam |
| 42ND | 42nd Street |
| REEDY | Reedy Creek |
| BELLE | Belle Isle |
| TRED | Tredegar / Canal Walk |
| BROWN | Brown's Island |
| MAYO | Mayo Bridge |
| ZDAM | Z-Dam area |
| PIPE | Pipeline Rapids |
| ANC | Ancarrow's Landing |
| OFFLIST | a real, named place on the river that isn't on this list |
| NONE | the note doesn't say where the person was |

- **"About 200 yards below X"** counts as X.
- **Several places in one note:** label only the place where the person was found.
- **Not sure:** if you, knowing what you meant, can't point to one site, use NONE.

## How to fill in the file

Open `holdout_notes.csv` in Excel. Add one row per note:

| note_id | author | gold_site | note |
|---|---|---|---|
| DV01 | Divya | REEDY | E05 on scene 1430, kayaker pinned below the Reedy put-in... |

- **note_id:** your initials plus a number (DV01, DV02... / JN01... / PR01...).
- Commas inside a note are fine.
- Save as **CSV UTF-8**, keeping the same file name, and tell Matt when you're done.

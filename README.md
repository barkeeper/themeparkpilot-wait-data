# themeparkpilot-wait-data

Auto-generated, **free ($0)** historical wait-time archive for **Theme Park
Pilot**. This is a data-only mirror — no app code lives here.

An hourly [GitHub Action](.github/workflows/wait-history.yml) records the
public [themeparks.wiki](https://api.themeparks.wiki/) live feed for every
park and commits it here. Public repos get unlimited Actions minutes, so
the whole pipeline is free; the Theme Park Pilot app + admin read these raw
files to draw their wait-history charts. No backend, no database.

## Layout

```
data/wait-history/
  index.json                 # parks that currently have data
  <parkId>.json              # 30-day rolling raw window (per sample)
  <parkId>.summary.json      # per-ride, per-UTC-day {min,max,avg,count}, kept FOREVER
```

- `<parkId>.json` is bounded (30 days) so no file grows without limit.
- `<parkId>.summary.json` is tiny and never trimmed — the months/years
  signal only ever gets richer (each day is frozen once complete).

## Reading it

```
https://raw.githubusercontent.com/barkeeper/themeparkpilot-wait-data/main/data/wait-history/<parkId>.summary.json
```

Park ids come from `https://api.themeparks.wiki/v1/destinations`.

## Config

`parks.json` is an optional allowlist — empty means **all** parks (the
default). The ingest logic is `ingest.py` (Python stdlib only), mirrored
from `tools/wait_history/ingest.py` in the main app repo.

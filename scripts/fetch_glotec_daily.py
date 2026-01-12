#!/usr/bin/env python3
"""
Fetch NOAA SWPC GloTEC GeoJSON (3-hourly targets) for a given UTC day (default: yesterday UTC),
save under data/YYYYMMDD/, and prune old day folders (keep last N days; default: 30).

Env:
  OUT_DIR        : output root directory (default: "data")
  DAY_UTC        : target day in YYYYMMDD (optional; default: yesterday UTC)
  KEEP_DAYS      : how many days to keep (default: 30)
  DRY_RUN_PRUNE  : "1" to only print prune actions (default: 0)
"""

import json
import os
import re
import shutil
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

LIST_URL = "https://services.swpc.noaa.gov/products/glotec/geojson_2d_urt.json"
BASE_DIR_URL = "https://services.swpc.noaa.gov/products/glotec/geojson_2d_urt/"

FNAME_RE = re.compile(r"glotec_icao_(\d{8})T(\d{6})Z\.geojson$")
DAYDIR_RE = re.compile(r"^\d{8}$")  # YYYYMMDD


def http_get(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "github-actions-glotec-fetch/1.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def parse_listing(list_json: Any) -> list[dict]:
    """
    Parse geojson_2d_urt.json listing.
    Expected: array of dict or strings. Handle both robustly.
    """
    if not isinstance(list_json, list):
        raise ValueError("Listing JSON is not a list.")

    items: list[dict] = []
    for it in list_json:
        name = None
        url = None

        if isinstance(it, str):
            name = it
            url = BASE_DIR_URL + name
        elif isinstance(it, dict):
            name = it.get("name") or it.get("file") or it.get("filename") or it.get("path")
            url = it.get("url") or it.get("href")
            if name and not url:
                url = BASE_DIR_URL + name
        else:
            continue

        if not name:
            continue

        m = FNAME_RE.search(name)
        if not m:
            continue

        ymd, hms = m.group(1), m.group(2)
        ts = datetime.strptime(ymd + hms, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        items.append({"name": name, "url": url, "ts": ts})

    items.sort(key=lambda x: x["ts"])
    return items


def choose_3hourly(items_for_day: list[dict], day_utc: datetime) -> list[dict]:
    """
    Choose nearest files to targets: 00:00, 03:00, ..., 21:00 UTC (8 targets).
    Removes duplicates while preserving order.
    """
    targets = [day_utc + timedelta(hours=h) for h in range(0, 24, 3)]
    chosen: list[dict] = []
    for t in targets:
        best = min(items_for_day, key=lambda x: abs((x["ts"] - t).total_seconds()))
        chosen.append(best)

    uniq: list[dict] = []
    seen: set[str] = set()
    for x in chosen:
        if x["name"] in seen:
            continue
        seen.add(x["name"])
        uniq.append(x)
    return uniq


def safe_write_bytes(path: str, data: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def ensure_day_dir(out_dir: str, day_utc: datetime) -> str:
    day_dir = os.path.join(out_dir, day_utc.strftime("%Y%m%d"))
    os.makedirs(day_dir, exist_ok=True)
    return day_dir


def prune_old_day_folders(out_dir: str, keep_days: int, dry_run: bool) -> None:
    """
    Keep last 'keep_days' day folders (UTC date based on folder name YYYYMMDD),
    remove older day folders under out_dir.
    """
    if keep_days <= 0:
        print("[PRUNE] keep_days <= 0 => skip pruning")
        return

    today_utc = datetime.now(timezone.utc).date()
    cutoff_date = today_utc - timedelta(days=keep_days - 1)  # inclusive keep window

    if not os.path.isdir(out_dir):
        return

    removed = 0
    scanned = 0

    for name in sorted(os.listdir(out_dir)):
        p = os.path.join(out_dir, name)
        if not os.path.isdir(p):
            continue
        if not DAYDIR_RE.match(name):
            continue

        scanned += 1
        try:
            d = datetime.strptime(name, "%Y%m%d").date()
        except ValueError:
            continue

        if d < cutoff_date:
            if dry_run:
                print(f"[PRUNE][DRY] would remove {p} (date {d.isoformat()} < cutoff {cutoff_date.isoformat()})")
            else:
                print(f"[PRUNE] removing {p} (date {d.isoformat()} < cutoff {cutoff_date.isoformat()})")
                shutil.rmtree(p, ignore_errors=True)
            removed += 1

    print(f"[PRUNE] scanned={scanned}, removed={removed}, keep_days={keep_days}, cutoff={cutoff_date.isoformat()}")


def main() -> int:
    out_dir = os.environ.get("OUT_DIR", "data")
    keep_days = int(os.environ.get("KEEP_DAYS", "30"))
    dry_run_prune = os.environ.get("DRY_RUN_PRUNE", "0") == "1"

    # Target day (UTC): default is yesterday UTC
    day = os.environ.get("DAY_UTC")
    if day:
        try:
            day_utc = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print("[ERROR] DAY_UTC must be YYYYMMDD", file=sys.stderr)
            return 2
    else:
        now = datetime.now(timezone.utc)
        y = (now - timedelta(days=1)).date()
        day_utc = datetime(y.year, y.month, y.day, tzinfo=timezone.utc)

    day_dir = ensure_day_dir(out_dir, day_utc)

    # Fetch listing
    listing_raw = http_get(LIST_URL)
    listing_json = json.loads(listing_raw.decode("utf-8"))
    items = parse_listing(listing_json)

    # Filter items for the day
    start = day_utc
    end = day_utc + timedelta(days=1)
    items_for_day = [x for x in items if start <= x["ts"] < end]

    if not items_for_day:
        print(f"[WARN] No items found for {day_utc.strftime('%Y%m%d')} (UTC).", file=sys.stderr)
        prune_old_day_folders(out_dir, keep_days=keep_days, dry_run=dry_run_prune)
        return 0

    chosen = choose_3hourly(items_for_day, day_utc)

    meta = {
        "day_utc": day_utc.strftime("%Y-%m-%d"),
        "source_list": LIST_URL,
        "saved": [],
    }

    for x in chosen:
        dest = os.path.join(day_dir, x["name"])
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            print(f"[SKIP] exists {dest}")
        else:
            print(f"[GET ] {x['url']} -> {dest}")
            data = http_get(x["url"])
            safe_write_bytes(dest, data)

        meta["saved"].append(
            {
                "name": x["name"],
                "ts_utc": x["ts"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "url": x["url"],
            }
        )

    index_path = os.path.join(day_dir, "index.json")
    safe_write_bytes(
        index_path,
        json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
    )

    print(f"[OK] saved {len(meta['saved'])} files into {day_dir}")

    prune_old_day_folders(out_dir, keep_days=keep_days, dry_run=dry_run_prune)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


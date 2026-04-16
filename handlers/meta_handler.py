"""
Hong Kong HKO metadata ingest

Scrapes station metadata from the HKO station list page,
converts DMS coordinates to decimal, and registers with station_lookup.
Supports local mode + safe STID generation.
"""

import re
import json
import time
import logging
import requests
import os
from bs4 import BeautifulSoup

from ingestlib.metadata import MetadataIngest
from ingestlib.core import make_lambda_handler
from config import NAME, MNET_ID, STID_PREFIX, INCOMING_ELEVATION_UNIT


# ── Network helpers (HKO-specific) ────────────────────────────────

HKO_STATION_URL = "https://www.hko.gov.hk/en/cis/stn.htm"

_META_MAX_RETRIES = 3
_META_BACKOFF     = 2.0  # seconds; wait = backoff * attempt

# ── Helpers ───────────────────────────────────────────────────────

def _normalize_code(code: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", code).upper()


def _build_stid(code: str, assigned: set, prefix: str) -> str:
    base = f"{prefix}{code}"
    stid = base[:10]

    if stid not in assigned:
        return stid

    for i in range(1, 100):
        suffix = str(i)
        trimmed = base[: 10 - len(suffix)]
        candidate = f"{trimmed}{suffix}"
        if candidate not in assigned:
            return candidate

    raise RuntimeError(f"Unable to generate unique STID for {code}")


def _dms_to_decimal(dms_str: str) -> float | None:
    """
    Convert a DMS coordinate string to decimal degrees.

    Handles formats such as:
        22°18'07"N    22°18'N    22°18.1'N
    Returns a rounded float or None if unparseable.
    """
    try:
        dms_str = dms_str.strip()
        match = re.match(
            r"(\d+)[°d]\s*(\d+(?:\.\d+)?)?['′]?\s*(\d+(?:\.\d+)?)?[\"″]?\s*([NSEW])?",
            dms_str,
        )
        if not match:
            return None

        degrees   = float(match.group(1))
        minutes   = float(match.group(2)) if match.group(2) else 0.0
        seconds   = float(match.group(3)) if match.group(3) else 0.0
        direction = match.group(4)

        decimal = degrees + minutes / 60.0 + seconds / 3600.0
        if direction in ("S", "W"):
            decimal *= -1

        return round(decimal, 6)
    except Exception:
        return None


# ── Fetch  ────────────────────────────────────

def fetch_hko_station_metadata(logger: logging.Logger) -> dict:
    """
    Fetch + parse HKO station metadata (with retries).
    Retries up to _META_MAX_RETRIES times with exponential back-off.

    Each entry::

        {
            "NAME":      str,
            "LATITUDE":  float,
            "LONGITUDE": float,
            "OTHER_ID":  str,
            "ELEVATION": float | None,
        }
    """
    resp = None
    for attempt in range(1, _META_MAX_RETRIES + 1):
        try:
            resp = requests.get(HKO_STATION_URL, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            wait = _META_BACKOFF * attempt
            if attempt < _META_MAX_RETRIES:
                time.sleep(wait)
            else:
                logger.error("ACQUIRE: all retries failed")
                return {}

    out = {}
    skipped = 0
    soup = BeautifulSoup(resp.text, "html.parser")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            try:
                name_cell = cols[0].get_text(strip=True)
                match = re.search(r"\((.*?)\)", name_cell)
                if not match:
                    continue

                code = _normalize_code(match.group(1))
                name = name_cell.split("(")[0].strip()

                lat = _dms_to_decimal(cols[1].get_text(strip=True))
                lon = _dms_to_decimal(cols[2].get_text(strip=True))

                if lat is None or lon is None:
                    skipped += 1
                    continue

                elevation = None
                try:
                    elevation = float(cols[3].get_text(strip=True))
                except Exception:
                    pass

                out[code] = {
                    "NAME": name,
                    "LATITUDE": lat,
                    "LONGITUDE": lon,
                    "ELEVATION": elevation,
                    "OTHER_ID": code,
                }

            except Exception:
                skipped += 1

    logger.info(f"ACQUIRE: scraped {len(out)} stations ({skipped} skipped)")
    return out


# ── The ingest ─────────────────────────────────────────────────────

class HongKongMeta(MetadataIngest):
    NAME           = "hongkong"
    MNET_ID        = MNET_ID
    STID_PREFIX    = STID_PREFIX
    ELEVATION_UNIT = INCOMING_ELEVATION_UNIT

    # ── Local mode helpers ───────────────────────────────────────

    def _is_local(self) -> bool:
        return os.environ.get("MODE") == "local"

    # ── Setup ───────────────────────────────────────────────────

    def setup(self):
        if self._is_local():
            self.logger.info("SETUP: LOCAL mode")

            path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "dev",
                f"{self.NAME}_stations_metadata.json",
            )

            if os.path.exists(path):
                with open(path) as f:
                    self.existing_stations = json.load(f)
                self.logger.info(f"Loaded {len(self.existing_stations)} stations (local)")
            else:
                self.existing_stations = {}
        else:
            super().setup()

    # ── Acquire (OLD STYLE CALL) ────────────────────────────────

    def acquire(self):
        return fetch_hko_station_metadata(self.logger)

    # ── Parse  ──────────────────────────────────

    def parse(self, raw_data: dict) -> dict:
        """
        Merge scraped HKO stations into existing_stations.

        Rules (mirrors Taiwan):
          - SYNOPTIC_STID is immutable once assigned.
          - Mutable fields (NAME / LAT / LON / OTHER_ID / ELEVATION) are
            updated from the fresh scrape.
          - Stations absent from today's scrape are retained as-is.
        """
        station_meta = dict(self.existing_stations)

        assigned_stids = {
            s.get("SYNOPTIC_STID")
            for s in self.existing_stations.values()
            if s.get("SYNOPTIC_STID")
        }

        new_count     = 0
        updated_count = 0

        for code, info in raw_data.items():
            existing = self.existing_stations.get(code)
            is_new = existing is None

            if existing and existing.get("SYNOPTIC_STID"):
                synoptic_stid = existing["SYNOPTIC_STID"]
            else:
                synoptic_stid = _build_stid(code, assigned_stids, self.STID_PREFIX)

            assigned_stids.add(synoptic_stid)

            station_meta[code] = {
                "SYNOPTIC_STID": synoptic_stid,
                "NAME":          info["NAME"],
                "LATITUDE":      info["LATITUDE"],
                "LONGITUDE":     info["LONGITUDE"],
                "ELEVATION":     info.get("ELEVATION"),
                "OTHER_ID":      code,
            }

            if is_new:
                new_count += 1
            else:
                updated_count += 1

        self.logger.info(
            f"PARSE: {new_count} new, {updated_count} updated, {len(station_meta)} total"
        )
        return station_meta

    # ── Save ────────────────────────────────────────────────────

    def save_station_meta(self, station_meta: dict):
        if self._is_local():
            path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "dev",
                f"{self.NAME}_stations_metadata.json",
            )
            os.makedirs(os.path.dirname(path), exist_ok=True)

            with open(path, "w") as f:
                json.dump(station_meta, f, indent=4)

            self.logger.info(f"LOCAL SAVE: wrote {len(station_meta)} stations")
        else:
            super().save_station_meta(station_meta)


# ── Entry points ─────────────────────────────────────────────────

lambda_handler = make_lambda_handler(HongKongMeta)

if __name__ == "__main__":
    import sys

    if "--local" in sys.argv:
        os.environ["MODE"] = "local"

    HongKongMeta().run()
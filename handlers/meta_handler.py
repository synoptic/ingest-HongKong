"""
Hong Kong HKO metadata ingest

Scrapes station metadata from the HKO station list page,
converts DMS coordinates to decimal, and registers with station_lookup.
"""

import re
import json
import time
import math
import logging
import requests
from bs4 import BeautifulSoup

from ingestlib.metadata import MetadataIngest
from ingestlib.core import make_lambda_handler
from config import NAME, MNET_ID, STID_PREFIX, INCOMING_ELEVATION_UNIT


# ── Network helpers (HKO-specific) ────────────────────────────────

HKO_STATION_URL = "https://www.hko.gov.hk/en/cis/stn.htm"

_META_MAX_RETRIES = 3
_META_BACKOFF     = 2.0  # seconds; wait = backoff * attempt


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


def fetch_hko_station_metadata(logger: logging.Logger) -> dict:
    """
    Scrape the HKO station list page and return a dict keyed by station code.

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
            logger.warning(
                f"ACQUIRE: attempt {attempt}/{_META_MAX_RETRIES} failed — {e}; "
                f"retrying in {wait:.1f}s"
            )
            if attempt < _META_MAX_RETRIES:
                time.sleep(wait)
            else:
                logger.error(f"ACQUIRE: all attempts exhausted for {HKO_STATION_URL}")
                return {}

    out: dict = {}
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

                code = match.group(1).strip()
                name = name_cell.split("(")[0].strip()

                lat = _dms_to_decimal(cols[1].get_text(strip=True))
                lon = _dms_to_decimal(cols[2].get_text(strip=True))

                if lat is None or lon is None:
                    logger.debug(
                        f"ACQUIRE: skipping '{name}' ({code}) — "
                        f"unparseable coordinates"
                    )
                    skipped += 1
                    continue

                elevation: float | None = None
                try:
                    elevation = float(cols[3].get_text(strip=True))
                except (ValueError, TypeError):
                    pass

                out[code] = {
                    "NAME":      name,
                    "LATITUDE":  lat,
                    "LONGITUDE": lon,
                    "OTHER_ID":  code,
                    "ELEVATION": elevation,
                }

            except Exception as e:
                logger.debug(f"ACQUIRE: skipping row — {e}")
                skipped += 1

    logger.info(
        f"ACQUIRE: scraped {len(out)} stations "
        f"({skipped} rows skipped due to parse errors)"
    )
    return out


# ── The ingest ─────────────────────────────────────────────────────

class HongKongMeta(MetadataIngest):
    NAME           = "hongkong"
    MNET_ID        = MNET_ID
    STID_PREFIX    = STID_PREFIX
    ELEVATION_UNIT = INCOMING_ELEVATION_UNIT

    def setup(self):
        """No auth key required — HKO station page is public."""
        self.logger.info("SETUP: HKO station page is public, no auth required")

    def acquire(self):
        """Scrape HKO station metadata page."""
        return fetch_hko_station_metadata(self.logger)

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
        new_count     = 0
        updated_count = 0

        for code, info in raw_data.items():
            existing = self.existing_stations.get(code, {})
            is_new   = code not in self.existing_stations

            # SYNOPTIC_STID is immutable — only assigned on first appearance
            synoptic_stid = (
                existing.get("SYNOPTIC_STID") or f"{self.STID_PREFIX}{code}"
            )

            station_meta[code] = {
                "SYNOPTIC_STID": synoptic_stid,
                "NAME":          info["NAME"],
                "LATITUDE":      info["LATITUDE"],
                "LONGITUDE":     info["LONGITUDE"],
                "ELEVATION":     info.get("ELEVATION"),
                "OTHER_ID":      info["OTHER_ID"],
            }

            if is_new:
                new_count += 1
            else:
                updated_count += 1

        retained_count = len(self.existing_stations) - (len(raw_data) - new_count)
        self.logger.info(
            f"PARSE: {new_count} new, {updated_count} updated, "
            f"{retained_count} retained (absent from today's scrape), "
            f"{len(station_meta)} total"
        )
        return station_meta


# ── Entry points ───────────────────────────────────────────────────

lambda_handler = make_lambda_handler(HongKongMeta)

if __name__ == "__main__":
    HongKongMeta().run()
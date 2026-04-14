"""
Hong Kong HKO observation ingest

Fetches real-time weather observations from HKO open-data CSV endpoints,
parses and unit-converts each variable, and submits to POE.

Dev workflow
------------
1. Fill in data_dictionary/variables.py with the HKO variable mappings.
   Run with --mode dev.  Check the raw data in ../dev/ cache.

2. Check ../dev/grouped_obs.txt.  The validator output in the log will
   show you what's off.

3. Once grouped_obs looks right, run with --mode prod.
"""

import csv
import json
import time
import logging
import requests
import posixpath
from io import StringIO
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from ingestlib.ingest import Ingest
from ingestlib import aws
from ingestlib.core import make_lambda_handler
from config import NAME
from config.variables import variables
from ingestlib import parse


# ── Endpoints ──────────────────────────────────────────────────────

HKO_API_ENDPOINTS = {
    "air_temp":          "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_temperature.csv",
    "mean_humidity":     "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_humidity.csv",
    "wind_speed":        "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_10min_wind.csv",
    "wind_gust":         "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_10min_wind.csv",
    "mean_pressure":     "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_pressure.csv",
    "solar_radiation":   "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_solar.csv",
    "diffuse_radiation": "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_solar.csv",
    "grass_min_temp":    "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_grass.csv",
    "visibility":        "https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=LTMV&lang=en&rformat=csv",
}

# CSV column that holds each label's observation value
LABEL_VALUE_COLUMN = {
    "air_temp":          "Air Temperature(degree Celsius)",
    "mean_humidity":     "Relative Humidity(percent)",
    "wind_speed":        "10-Minute Mean Speed(km/hour)",
    "wind_gust":         "10-Minute Maximum Gust(km/hour)",
    "mean_pressure":     "Mean Sea Level Pressure(hPa)",
    "solar_radiation":   "Global Solar Radiation(watt/square meter)",
    "diffuse_radiation": "Diffuse Radiation(watt/square meter)",
    "grass_min_temp":    "Grass Temperature(degree Celsius)",
    "visibility":        "10 minute mean visibility",
}

DATETIME_COL = "Date time"
STATION_COL  = "Automatic Weather Station"

# HTTP retry settings
_MAX_RETRIES    = 3
_BACKOFF_FACTOR = 1.5  # seconds; actual wait = backoff * attempt


# ── CSV name → metadata NAME aliases ──────────────────────────────
#
# HKO observation CSVs use short/different display names that do not always
# match the full station names stored in the metadata. Add new entries here
# whenever an unresolved_station appears in the PARSE summary log.

_CSV_NAME_ALIASES: dict[str, str] = {
    "hk observatory":      "Hong Kong Observatory",
    "chek lap kok":        "Hong Kong International Airport",
    "hk park":             "Hong Kong Park",
    "kai tak runway park": "Kai Tak Runway Park*",
    "tap mun":             "Tap Mun***",
    "tap mun east":        "Tap Mun East***",
    "tuen mun":            "Tuen Mun Government Offices",
    "star ferry":          "Star Ferry",
    "tsuen wan ho koon":   "Tsuen Wan",
    "hong kong sea school":"Shau Kei Wan",
    "central":             "Central Pier",
    "sai wan ho":          "Quarry Bay",
    "pak tam chung":       "Pak Tam Chung",
}


# ── Network helpers ────────────────────────────────────────────────

def _fetch_one(label: str, url: str) -> tuple[str, Optional[str]]:
    """Fetch a single HKO endpoint with retry / exponential back-off."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                wait = _BACKOFF_FACTOR * attempt * 2
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return label, resp.text
        except requests.RequestException as exc:
            wait = _BACKOFF_FACTOR * attempt
            if attempt < _MAX_RETRIES:
                time.sleep(wait)

    return label, None


def fetch_hko_data() -> Optional[dict]:
    """
    Fetch all HKO endpoints in parallel.

    De-duplicates requests: endpoints sharing a URL (wind_speed / wind_gust)
    are fetched once and the raw text reused for both labels.
    Returns dict {label: raw_csv_text}, or None if every endpoint fails.
    """
    url_to_labels: dict[str, list[str]] = defaultdict(list)
    for label, url in HKO_API_ENDPOINTS.items():
        url_to_labels[url].append(label)

    unique_items = [(labels[0], url) for url, labels in url_to_labels.items()]
    results: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=min(len(unique_items), 8)) as pool:
        futures = {
            pool.submit(_fetch_one, primary_label, url): url
            for primary_label, url in unique_items
        }
        for future in as_completed(futures):
            url = futures[future]
            primary_label, text = future.result()
            if text is None:
                continue
            for lbl in url_to_labels[url]:
                results[lbl] = text

    if not results:
        return None

    return results


# ── Parse helpers ──────────────────────────────────────────────────

def _build_name_to_stid_map(station_meta: dict, logger: logging.Logger) -> dict:
    """
    Build a case-insensitive map: csv_station_name (lower) → SYNOPTIC_STID.

    Two-pass: index metadata NAME fields first, then apply _CSV_NAME_ALIASES.
    """
    mapping: dict[str, str] = {}
    for _otid, record in station_meta.items():
        name = record.get("NAME", "")
        stid = record.get("SYNOPTIC_STID", "")
        if name and stid:
            mapping[name.lower().strip()] = stid
            clean = name.lower().strip().rstrip("*").strip()
            if clean not in mapping:
                mapping[clean] = stid

    for csv_name_lower, meta_name in _CSV_NAME_ALIASES.items():
        target = meta_name.lower().strip()
        if target in mapping:
            mapping[csv_name_lower] = mapping[target]
        else:
            logger.debug(
                f"STATION MAP: alias '{csv_name_lower}' → '{meta_name}' "
                f"could not be resolved (target not in metadata)"
            )

    logger.debug(f"STATION MAP: built {len(mapping)} name→stid entries")
    return mapping


def _parse_hko_datetime(raw: str) -> Optional[datetime]:
    """
    Parse HKO compact datetime: YYYYMMDDHHmm (12 digits, HKT = UTC+8).
    Returns a UTC datetime or None on failure.
    """
    raw = raw.strip()
    if len(raw) != 12 or not raw.isdigit():
        return None
    try:
        hkt = timezone(timedelta(hours=8))
        dt_hkt = datetime(
            int(raw[0:4]), int(raw[4:6]), int(raw[6:8]),
            int(raw[8:10]), int(raw[10:12]),
            tzinfo=hkt,
        )
        return dt_hkt.astimezone(timezone.utc)
    except ValueError:
        return None


def _parse_value(label: str, raw_val: str) -> Optional[float]:
    """
    Extract a numeric value from a raw CSV string and apply unit conversion
    driven by the variables data dictionary.
    """
    raw_val = raw_val.strip().strip('"')
    if label == "visibility":
        raw_val = raw_val.lower().replace("km", "").strip()
    if not raw_val or raw_val.upper() in ("N/A", "NA", ""):
        return None
    try:
        value = float(raw_val)
    except ValueError:
        return None

    incoming_unit = parse.get_translated_value(label, variables=variables, field="incoming_unit")
    final_unit    = variables[label]["final_unit"]

    if incoming_unit and incoming_unit != final_unit:
        try:
            conversion_name = parse.create_conversion(incoming_unit, variables, label)
            value = round(parse.convert_units(conversion_name, value), 3)
        except Exception:
            value = round(value, 3)
    else:
        value = round(value, 3)

    return value


# ── The ingest ─────────────────────────────────────────────────────

class HongKongIngest(Ingest):
    NAME = NAME

    HOURS_TO_RETAIN = 12
    CACHE_RAW_DATA  = True

    def setup(self):
        """Load variables — HKO endpoints are public, no auth required."""
        self.variables = variables
        self.logger.debug("SETUP: HKO endpoints are public, no auth required")

    def acquire(self):
        """Fetch all HKO CSV endpoints in parallel."""
        return fetch_hko_data()

    def parse(self, incoming_data: dict) -> dict:
        """
        Transform raw HKO CSV data into grouped_obs_set.

        For each label, reads the relevant CSV column, resolves the station
        name to a SYNOPTIC_STID via metadata, parses and converts the value,
        then merges into a grouped dict keyed by "STID|dattim".
        """
        name_to_stid = _build_name_to_stid_map(self.station_meta, self.logger)
        cutoff_dt    = datetime.now(timezone.utc) - timedelta(hours=self.HOURS_TO_RETAIN)
        counters     = defaultdict(int)

        grouped_obs_set: dict[str, dict] = {}

        for label, raw_csv in incoming_data.items():
            if not raw_csv:
                counters["empty_csv"] += 1
                continue

            var_config = self.variables.get(label)
            if not var_config:
                self.logger.warning(f"PARSE: no variable mapping for '{label}', skipping")
                counters["no_var_config"] += 1
                continue

            vargem = parse.get_translated_value(label, variables=self.variables, field="vargem")
            if not vargem:
                self.logger.warning(f"PARSE: could not resolve vargem for '{label}', skipping")
                counters["no_vargem"] += 1
                continue
            vnum = int(self.variables[label]["VNUM"])

            value_col = LABEL_VALUE_COLUMN.get(label)
            if not value_col:
                self.logger.warning(f"PARSE: no value-column mapping for '{label}', skipping")
                counters["no_col_mapping"] += 1
                continue

            try:
                clean_csv = raw_csv.lstrip("\ufeff")
                lines = clean_csv.splitlines()
                if not lines:
                    counters["empty_csv"] += 1
                    continue

                # Strip quotes from header line (visibility CSV wraps headers in quotes)
                lines[0] = ",".join(h.strip().strip('"') for h in lines[0].split(","))

                reader    = csv.DictReader(StringIO("\n".join(lines)))
                fieldnames = reader.fieldnames or []
                missing   = [c for c in (DATETIME_COL, STATION_COL, value_col) if c not in fieldnames]
                if missing:
                    self.logger.warning(
                        f"PARSE: '{label}' missing columns {missing} — found {fieldnames}; skipping"
                    )
                    counters["missing_columns"] += 1
                    continue

                for row in reader:
                    # datetime
                    dt_utc = _parse_hko_datetime(row.get(DATETIME_COL, ""))
                    if dt_utc is None:
                        counters["bad_datetime"] += 1
                        continue
                    if dt_utc < cutoff_dt:
                        counters["old_timestamp"] += 1
                        continue

                    dattim_str = dt_utc.strftime("%Y%m%d%H%M")

                    # station
                    station_name = row.get(STATION_COL, "").strip()
                    if not station_name:
                        counters["missing_station_name"] += 1
                        continue
                    synoptic_stid = name_to_stid.get(station_name.lower().strip())
                    if not synoptic_stid:
                        self.logger.debug(
                            f"PARSE [{label}]: '{station_name}' not in metadata, skipping"
                        )
                        counters["unresolved_station"] += 1
                        continue

                    # value
                    value = _parse_value(label, row.get(value_col, ""))
                    if value is None:
                        counters["missing_value"] += 1
                        continue

                    key = f"{synoptic_stid}|{dattim_str}"
                    grouped_obs_set.setdefault(key, {})[vargem] = {vnum: value}
                    counters["accepted"] += 1

            except Exception as exc:
                self.logger.warning(f"PARSE: unexpected error for '{label}': {exc}")
                counters["parse_error"] += 1

        self.logger.info(
            f"PARSE summary — "
            f"accepted={counters['accepted']}, "
            f"old_timestamp={counters['old_timestamp']}, "
            f"unresolved_station={counters['unresolved_station']}, "
            f"missing_value={counters['missing_value']}, "
            f"bad_datetime={counters['bad_datetime']}, "
            f"missing_station_name={counters['missing_station_name']}, "
            f"missing_columns={counters['missing_columns']}, "
            f"parse_error={counters['parse_error']}"
        )
        self.logger.debug(f"PARSE: {len(grouped_obs_set)} grouped observation records")
        return grouped_obs_set


# ── Entry points ───────────────────────────────────────────────────

lambda_handler = make_lambda_handler(HongKongIngest)

if __name__ == "__main__":
    HongKongIngest().run()
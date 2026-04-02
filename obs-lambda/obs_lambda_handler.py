import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from ingestlib import poe, parse, aws, validator, metamgr, core
from data_dictionary import variables
import os
import time
import json
import posixpath
import requests
from collections import defaultdict
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
########################################################################################################################
# DEFINE LOGSTREAMS AND CONSTANTS
########################################################################################################################
INGEST_NAME = 'hongkong'
logger = logging.getLogger(f"{INGEST_NAME}_ingest")
 
HKO_API_ENDPOINTS = {
    "air_temp":               "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_temperature.csv",
    "mean_humidity":          "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_humidity.csv",
    "wind_speed":             "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_10min_wind.csv",
    "wind_gust":              "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_10min_wind.csv",
    "mean_pressure":          "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_pressure.csv",
    "solar_radiation":        "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_solar.csv",
    "diffuse_radiation":      "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_solar.csv",
    "grass_min_temp":         "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_grass.csv",
    "visibility":             "https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=LTMV&lang=en&rformat=csv",
}

PREVIOUS_HOURS_TO_RETAIN = 12  # hours to retain

# ---------------------------------------------------------------------------
# Column name mappings — each label maps to the exact CSV column header.
# ---------------------------------------------------------------------------
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

# HKO date-time and station column headers
DATETIME_COL = "Date time"
STATION_COL  = "Automatic Weather Station"

# HTTP retry settings
_MAX_RETRIES   = 3
_BACKOFF_FACTOR = 1.5   # seconds; actual wait = backoff * attempt

########################################################################################################################
# STATION-NAME → SYNOPTIC_STID  (uses metadata, not synthetic IDs)
########################################################################################################################

# ---------------------------------------------------------------------------
# CSV name → metadata NAME aliases
#
# HKO observation CSVs use short/different display names that do not always
# match the full station names stored in the metadata.  This table maps every
# known CSV variant (lower-cased) to the exact NAME value used in metadata.
#
# How to extend: if a new unresolved_station appears in the PARSE summary,
# add  "csv name (lower)": "metadata NAME (exact, case-insensitive)"  here.
# ---------------------------------------------------------------------------
_CSV_NAME_ALIASES: dict[str, str] = {
    # Observation CSV name          : Metadata NAME
    "hk observatory"               : "Hong Kong Observatory",
    "chek lap kok"                 : "Hong Kong International Airport",
    "hk park"                      : "Hong Kong Park",
    "kai tak runway park"          : "Kai Tak Runway Park*",
    "tap mun"                      : "Tap Mun***",
    "tap mun east"                 : "Tap Mun East***",
    "tuen mun"                     : "Tuen Mun Government Offices",
    "star ferry"                   : "Star Ferry",
    "tsuen wan ho koon"            : "Tsuen Wan",
    "hong kong sea school"         : "Shau Kei Wan",   # HKO uses this alias for SKW
    "central"                      : "Central Pier",
    "sai wan ho"                   : "Quarry Bay",     # closest mapped station
    "pak tam chung"                : "Pak Tam Chung",  # OTID = Tsak Yue Wu in meta
}


def _build_name_to_stid_map(station_meta: dict) -> dict:
    """
    Build a case-insensitive map: csv_station_name (lower) → SYNOPTIC_STID.

    Two-pass approach:
      1. Index every metadata record by its NAME field (primary mapping).
      2. Apply _CSV_NAME_ALIASES so that short/alternate CSV display names
         resolve correctly even when they differ from the metadata NAME.

    Falls back gracefully — unresolved names are logged and skipped by the
    caller so a missing alias never causes a silent data loss without a trace.
    """
    # Pass 1: name (lower) → SYNOPTIC_STID from metadata
    mapping: dict[str, str] = {}
    for _otid, record in station_meta.items():
        name = record.get("NAME", "")
        stid = record.get("SYNOPTIC_STID", "")
        if name and stid:
            mapping[name.lower().strip()] = stid
            # Also strip trailing punctuation variants (e.g. "Tap Mun***" → "tap mun")
            clean = name.lower().strip().rstrip("*").strip()
            if clean not in mapping:
                mapping[clean] = stid

    # Pass 2: resolve aliases → look up the target NAME in the mapping we just built
    for csv_name_lower, meta_name in _CSV_NAME_ALIASES.items():
        target = meta_name.lower().strip()
        if target in mapping:
            mapping[csv_name_lower] = mapping[target]
        else:
            logger.debug(
                f"STATION MAP: alias '{csv_name_lower}' → '{meta_name}' "
                f"could not be resolved (target not in metadata)"
            )

    logger.debug(f"STATION MAP: built {len(mapping)} name→stid entries from metadata")
    return mapping


########################################################################################################################
# FETCH
########################################################################################################################

def _fetch_one(label: str, url: str) -> tuple[str, Optional[str]]:
    """
    Fetch a single HKO endpoint with retry / exponential back-off.

    Returns (label, text) on success or (label, None) on failure.
    Rate-limit responses (HTTP 429) trigger a longer wait before retrying.
    """
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)

            if resp.status_code == 429:
                wait = _BACKOFF_FACTOR * attempt * 2
                logger.warning(f"FETCH [{label}]: rate-limited (429), waiting {wait:.1f}s (attempt {attempt})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            logger.debug(f"FETCH [{label}]: OK ({len(resp.text)} chars) on attempt {attempt}")
            return label, resp.text

        except requests.RequestException as exc:
            wait = _BACKOFF_FACTOR * attempt
            logger.warning(f"FETCH [{label}]: attempt {attempt}/{_MAX_RETRIES} failed — {exc}; retrying in {wait:.1f}s")
            if attempt < _MAX_RETRIES:
                time.sleep(wait)

    logger.error(f"FETCH [{label}]: all {_MAX_RETRIES} attempts failed, skipping")
    return label, None


def fetch_hko_data() -> Optional[dict]:
    """
    Fetch all HKO API endpoints in parallel using a thread pool.

    De-duplicates requests: endpoints that share a URL (wind_speed and
    wind_gust both hit the wind CSV) are fetched only once and the raw
    text is reused for both labels, saving a redundant network round-trip.

    Returns dict {label: raw_csv_text} — labels with failed fetches are
    absent from the dict.  Returns None only if every single endpoint fails.
    """
    # Group labels by URL to avoid duplicate fetches
    url_to_labels: dict[str, list[str]] = defaultdict(list)
    for label, url in HKO_API_ENDPOINTS.items():
        url_to_labels[url].append(label)

    unique_items = [(labels[0], url) for url, labels in url_to_labels.items()]

    results: dict[str, str] = {}  # label → raw text

    with ThreadPoolExecutor(max_workers=min(len(unique_items), 8)) as pool:
        futures = {pool.submit(_fetch_one, primary_label, url): url
                   for primary_label, url in unique_items}

        for future in as_completed(futures):
            url = futures[future]
            primary_label, text = future.result()
            if text is None:
                continue
            # Distribute the same raw text to all labels sharing this URL
            for lbl in url_to_labels[url]:
                results[lbl] = text

    if not results:
        logger.error("FETCH: all HKO endpoint requests failed")
        return None
 
    fetch_ok  = len(results)
    fetch_all = len(HKO_API_ENDPOINTS)
    logger.info(f"FETCH: {fetch_ok}/{fetch_all} labels retrieved successfully")
    return results
 
 
########################################################################################################################
# DEFINE ETL/PARSING FUNCTIONS
########################################################################################################################
def _csv_to_structured(label: str, raw_csv: str) -> dict:
    """Convert raw CSV to structured dict."""
    import csv
    from io import StringIO
    lines = raw_csv.splitlines()
    records = []
    if lines:
        reader = csv.DictReader(StringIO('\n'.join(lines)))
        for row in reader:
            cleaned = {k.strip(): (v.strip() if v is not None else "") for k, v in row.items() if k}
            records.append(cleaned)
 
    return {
        "label": label,
        "data": {
            "records": records,
            "record_count": len(records),
        },
    }
 

def cache_raw_data_simple(incoming_data: dict, work_dir: str,
                          s3_bucket_name: str, s3_prefix: str) -> bool:
    """Upload raw (structured) data snapshot to S3 for replay / audit."""
    if not incoming_data:
        logger.debug("CACHE: no incoming data; skipping")
        return False
 
        # Create timestamp-based filename
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        # Create S3 path: prefix/YYYY/MM/YYYYMMDD_HHMMSS.json
        year_month = datetime.now(timezone.utc).strftime('%Y/%m')
        s3_key = f"{s3_prefix}/{year_month}/{timestamp}.json"
 
        # Local file setup
        os.makedirs(work_dir, exist_ok=True)
    try:
        with open(local_path, 'w', encoding='utf-8') as fh:
            json.dump(incoming_data, fh, indent=2, ensure_ascii=False)
        aws.S3.upload_file(local_path, s3_bucket_name, s3_key)
        logger.debug(f"CACHE: uploaded to s3://{s3_bucket_name}/{s3_key}")
        return True
    except Exception as exc:
        logger.error(f"CACHE: failed — {exc}")
        return False


########################################################################################################################
# SEEN-OBS HELPERS  (with pruning)
########################################################################################################################

def load_seen_obs(filepath: str) -> dict:
    """
    Load the seen-obs store from disk.

    Format on disk: JSON object mapping  "STID|dattim|vargem" → ISO timestamp
    (the wall-clock time we first sent that key to POE).  Using a dict (rather
    than a plain set) lets us prune by age without re-fetching anything.

    Falls back to an empty dict when the file doesn't exist yet (first run).
    """
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, 'r') as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
        # Legacy plain-set format (list of strings) — migrate transparently
        now_iso = datetime.now(timezone.utc).isoformat()
        return {k: now_iso for k in data}
    except Exception as exc:
        logger.warning(f"SEEN-OBS: could not load {filepath}: {exc} — starting fresh")
        return {}


def save_seen_obs(filepath: str, seen: dict) -> None:
    """Persist the seen-obs dict to disk."""
    with open(filepath, 'w') as fh:
        json.dump(seen, fh)


def prune_seen_obs(seen: dict, cutoff_dt: datetime) -> dict:
    """
    Remove every entry whose recorded send-time is older than *cutoff_dt*.

    This prevents the seen-obs file from growing forever and eventually
    blocking all new data.  Any key older than PREVIOUS_HOURS_TO_RETAIN hours
    is dropped; the next run will re-evaluate it against live data (which
    at that point won't contain such old timestamps anyway).
    """
    before = len(seen)
    pruned = {}
    for key, sent_iso in seen.items():
        try:
            sent_dt = datetime.fromisoformat(sent_iso)
            if sent_dt >= cutoff_dt:
                pruned[key] = sent_iso
        except Exception:
            # Unparseable timestamp — keep the key to be safe
            pruned[key] = sent_iso

    removed = before - len(pruned)
    if removed:
        logger.info(f"SEEN-OBS: pruned {removed} stale keys ({before} → {len(pruned)})")
    return pruned


########################################################################################################################
# PARSE
########################################################################################################################
def _parse_hko_datetime(raw: str) -> Optional[datetime]:
    """
    Parse HKO compact datetime string: YYYYMMDDHHmm (12 digits, HKT = UTC+8).
    Returns a UTC datetime or None on failure.
    """
    raw = raw.strip()
    if len(raw) != 12 or not raw.isdigit():
        return None
    try:
        # HKO timestamps are in Hong Kong Time (UTC+8)
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
    Extract a numeric value from a raw string, apply unit conversion.

    Uses parse.get_translated_value / parse.create_conversion /
    parse.convert_units driven by the variables data dictionary.
    Returns converted float or None for missing / non-numeric input.
    """
    # Strip surrounding whitespace and quotes
    raw_val = raw_val.strip().strip('"')

    if label == "visibility":
        raw_val = raw_val.lower().replace("km", "").strip()

    if not raw_val or raw_val.upper() in ("N/A", "NA", ""):
        return None

    try:
        value = float(raw_val)
    except ValueError:
        return None

    # Resolve units from the variables dict
    incoming_unit = parse.get_translated_value(label, variables=variables, field='incoming_unit')
    final_unit    = variables[label]['final_unit']

    # Apply conversion only when the units differ
    if incoming_unit and incoming_unit != final_unit:
        try:
            conversion_name = parse.create_conversion(incoming_unit, variables, label)
            value = round(parse.convert_units(conversion_name, value), 3)
        except Exception:
            value = round(value, 3)
    else:
        value = round(value, 3)

    return value


# PARSE  (uses metadata for station lookup; dedup key includes vargem)
def parse_hko_data(incoming_data: dict, station_meta: dict) -> dict:
    import csv
    from io import StringIO
 
    name_to_stid = _build_name_to_stid_map(station_meta)

    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)

    # Counters for INFO-level reporting
    counters = defaultdict(int)
    grouped_obs_set: dict[str, dict] = {}

    for label, raw_csv in incoming_data.items():
        if not raw_csv:
            counters['empty_csv'] += 1
            continue

        var_config = variables.get(label)
        if not var_config:
            logger.warning(f"PARSE: No variable mapping for label '{label}', skipping")
            counters['no_var_config'] += 1
            continue

        vargem = parse.get_translated_value(label, variables=variables, field='vargem')
        if not vargem:
            logger.warning(f"PARSE: Could not resolve vargem for label '{label}', skipping")
            counters['no_vargem'] += 1
            continue
        vnum = int(variables[label]['VNUM'])

        # Identify which CSV column holds this label's value
        value_col = LABEL_VALUE_COLUMN.get(label)
        if not value_col:
            logger.warning(f"PARSE: No value-column mapping for label '{label}', skipping")
            counters['no_col_mapping'] += 1
            continue

        try:
            # Strip UTF-8 BOM if present
            clean_csv = raw_csv.lstrip('\ufeff')
            lines = clean_csv.splitlines()
            if not lines:
                logger.warning(f"PARSE: Empty CSV body for '{label}', skipping")
                counters['empty_csv'] += 1
                continue

            # Strip quotes from header line (visibility CSV wraps headers in quotes)
            lines[0] = ','.join(h.strip().strip('"') for h in lines[0].split(','))

            reader = csv.DictReader(StringIO('\n'.join(lines)))

            # Validate that the expected columns exist
            fieldnames = reader.fieldnames or []
            missing = [c for c in (DATETIME_COL, STATION_COL, value_col) if c not in fieldnames]
            if missing:
                logger.warning(
                    f"PARSE: CSV for '{label}' is missing expected columns {missing}. "
                    f"Found: {fieldnames}. Skipping label."
                )
                counters['missing_columns'] += 1
                continue

            rows = list(reader)
            logger.debug(f"PARSE [{label}]: {len(rows)} rows, value_col='{value_col}'")

            for row in rows:
                # --- datetime ---
                raw_dt = row.get(DATETIME_COL, "").strip()
                dt_utc = _parse_hko_datetime(raw_dt)
                if dt_utc is None:
                    logger.debug(f"PARSE [{label}]: unparseable datetime '{raw_dt}', skipping row")
                    counters['bad_datetime'] += 1
                    continue

                if dt_utc < cutoff_dt:
                    counters['old_timestamp'] += 1
                    continue

                dattim_str = dt_utc.strftime("%Y%m%d%H%M")

                # --- station lookup via metadata ---
                station_name = row.get(STATION_COL, "").strip()
                if not station_name:
                    counters['missing_station_name'] += 1
                    continue

                synoptic_stid = name_to_stid.get(station_name.lower().strip())
                if not synoptic_stid:
                    logger.debug(
                        f"PARSE [{label}]: station '{station_name}' not found in metadata, skipping row"
                    )
                    counters['unresolved_station'] += 1
                    continue

                # --- value ---
                raw_val = row.get(value_col, "")
                value = _parse_value(label, raw_val)
                if value is None:
                    counters['missing_value'] += 1
                    continue

                # --- merge into grouped dict ---
                key = f"{synoptic_stid}|{dattim_str}"
                grouped_obs_set.setdefault(key, {})[vargem] = {vnum: value}
                counters['accepted'] += 1

        except Exception as exc:
            logger.warning(f"PARSE: Unexpected error parsing '{label}': {exc}")
            counters['parse_error'] += 1
            continue

    # INFO-level summary so operators always see what happened
    logger.info(
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
    logger.debug(f"PARSE: created {len(grouped_obs_set)} grouped observation records")
    return grouped_obs_set
 
 
########################################################################################################################
# MAIN FUNCTION
########################################################################################################################
def main(event, context):
    from args import args
 
    # --- decide dirs once ---
    if args.local_run or args.dev:
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
        work_dir = "../dev/"
        s3_work_dir = "dev_tmp/"
    else:
        log_dir = "/tmp/tmp/"
        work_dir = "/tmp/tmp/"
        s3_work_dir = "tmp/"
 
    # --- logging ---
    log_file = core.setup_logging(
        logger, INGEST_NAME,
        log_level=getattr(args, "log_level", "INFO"),
        write_logs=True,
        log_dir=log_dir,                        # where the file lives (dev or prod)
        filename=f"{INGEST_NAME}_obs.log",      # stable name for S3 overwrite
    )
 
    # --- signals ---
    core.setup_signal_handler(logger, args)
 
    logger.debug(f"poe socket: {args.poe_socket_address}")
    logger.debug(f"poe socket port: {args.poe_socket_port}")
 
    start_runtime = time.time()
    success_flag = 0
    s3_bucket_name = None  # ensure defined for finally block
 
    try:
        # paths
        os.makedirs(work_dir, exist_ok=True)
        s3_bucket_name       = os.environ["INTERNAL_BUCKET_NAME"]
        cache_s3_bucket_name = os.environ.get("CACHE_S3_BUCKET_NAME", "synoptic-ingest-provider-data-cache-a4fb6")
 
        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")
        s3_seen_obs_file   = posixpath.join(s3_work_dir, "seen_obs.txt")
        seen_obs_file      = os.path.join(work_dir, "seen_obs.txt")
        station_meta_file  = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)

        # ── download seen-obs ──────────────────────────────────────────────
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_seen_obs_file,local_directory=work_dir)
        except Exception as exc:
            logger.warning(f"SEEN-OBS: could not download {s3_seen_obs_file}: {exc}")

        # ── load station metadata ──────────────────────────────────────────
        # On a local run the meta handler writes directly to dev/ and skips
        # the S3 upload, so we read from dev/ directly.  In production we
        # always download the authoritative copy from S3 first.
        station_meta: dict = {}

        if args.local_run or args.dev:
            # Resolve the dev directory relative to this file so it works
            # regardless of the working directory the test is launched from.
            dev_dir           = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
            station_meta_file = os.path.join(dev_dir, f"{INGEST_NAME}_stations_metadata.json")
            logger.debug(f"STATION META: local run — reading from {station_meta_file}")
        # else: station_meta_file already set to the work_dir path above

        if not (args.local_run or args.dev):
            try:
                aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_station_meta_file, local_directory=work_dir)

            except Exception as e:
                logger.warning(f"STATION META: could not download {s3_station_meta_file}. Error: {e}")
 
        ########################################################################################################################
        # GET LATEST OBS
        ########################################################################################################################
 
        # load station metadata file
        if os.path.exists(station_meta_file):
            station_meta = parse.load_json_file(file_path=station_meta_file)
            logger.info(f"STATION META: loaded {len(station_meta)} stations")
        else:
            logger.warning(
                f"STATION META: file not found at {station_meta_file} — "
                "run test_meta_lambda.py first to generate it"
            )

        # ── load + prune seen-obs ──────────────────────────────────────────
        seen_obs = load_seen_obs(seen_obs_file)
        seen_obs = prune_seen_obs(seen_obs, cutoff_dt)
        logger.info(f"SEEN-OBS: {len(seen_obs)} entries after pruning")

        # FETCH
        logger.debug("FETCH: starting parallel data fetch")
        incoming_data = fetch_hko_data()
        logger.debug(f"FETCH: complete — got data? {bool(incoming_data)}")

        # cache structured snapshot
        if incoming_data:
            cache_data = {
                label: _csv_to_structured(label, raw_csv)
                for label, raw_csv in incoming_data.items()
            }
 
        # Store raw incoming data in cache
            cache_raw_data_simple(
                incoming_data=cache_data,
                work_dir=work_dir,
                s3_bucket_name=cache_s3_bucket_name,
                s3_prefix=INGEST_NAME
            )
 
        if not incoming_data:
            logger.error(msg=json.dumps({'Incoming_Data_Success': 0}))
            return

        logger.info(json.dumps({'Incoming_Data_Success': 1}))


        # Parse incoming data into a grouped dict (station|dattim)
        grouped_obs_set = parse_hko_data(incoming_data, station_meta)

        if not grouped_obs_set:
            logger.warning("PARSE: No grouped observations generated — nothing to send to POE")
            return

        # Format for POE: "STID|dattim|{json_blob_no_spaces}"
        grouped_obs = [
            f"{k}|{json.dumps(v, separators=(',', ':'))}"
            for k, v in grouped_obs_set.items()
        ]

        ####################################################################
        # VALIDATE DATA
        ####################################################################
        # save the grouped obs and station meta if it exists
        if args.local_run or args.dev:
            dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
            os.makedirs(dev_dir, exist_ok=True)
 
            # Save grouped_obs as a text file (one line per observation string)
            with open(os.path.join(dev_dir, 'grouped_obs.txt'), 'w') as f:
                for obs in sorted(grouped_obs):
                    f.write(obs + '\n')
            logger.debug(f"[DEV] Saved grouped_obs ({len(grouped_obs)} lines)")
 
            with open(os.path.join(dev_dir, 'station_meta.json'), 'w') as f:
                json.dump(station_meta, f, indent=4)
            logger.debug("[DEV] Saved station_meta snapshot")

 
        # Try to fetch variables_table unless local_run
        variables_table = {}
        if not args.local_run:
            try:
                variables_table = metamgr.grab_variables_table(
                    socket_address=args.metamgr_socket_address,
                    socket_port=args.metamgr_socket_port,
                )
            except Exception as e:
                    logger.warning(f"[VALIDATION] Skipping variable-table-based checks: {e}")
 
        # Variable validations
        variable_validators = [
            validator.validate_vargem_vnums,
            validator.validate_statistic_context_vnum,
            validator.validate_required_variable_fields,
            validator.validate_overlapping_variable_names,
        ]
        if variables_table:
            variable_validators.append(
                partial(validator.validate_variables, variables_table=variables_table)
            )

        end_time   = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)
        # Observation validations
        obs_validators = [
            lambda obs: validator.validate_dattim(obs, start_time, end_time)
        ]
        if variables_table:
            obs_validators.append(partial(validator.validate_observation_ranges, variables_table=variables_table))
 
        # Run validations
        all_validation_messages = []
        for vfunc in variable_validators:
            for m in vfunc(variables):
                all_validation_messages.append((getattr(vfunc, '__name__', str(vfunc)), m))
        for ofunc in obs_validators:
            for m in ofunc(grouped_obs):
                all_validation_messages.append((getattr(ofunc, '__name__', str(ofunc)), m))
 
        if all_validation_messages:
            grouped_msgs = defaultdict(list)
            for name, msg in all_validation_messages:
                grouped_msgs[name].append(msg)
            for func_name, msgs in grouped_msgs.items():
                logger.debug(f"[{func_name}] {len(msgs)} issue(s)")
                for m in msgs:
                    logger.debug(f"  [{func_name}] {m}")
        else:
            logger.debug("VALIDATION: all checks passed")
 
        ####################################################################
        # DEDUP + SEND TO POE
        #
        # Dedup key: "STID|dattim|vargem"
        #
        # Using a composite key that includes the variable name means a new
        # variable arriving for an already-seen station+timestamp is NOT
        # silently dropped — it will be forwarded to POE correctly.
        #
        # We pass seen_obs (the full dict keyed by composite key) into
        # poe_formatter as the prior set so poe's own dedup logic can also
        # operate correctly.  We then record every new key with its send
        # timestamp so we can prune by age on the next run.
        ####################################################################
        new_obs_keys: dict[str, str] = {}  # key → ISO send-time
        send_time_iso = datetime.now(timezone.utc).isoformat()

        # Build the set of composite keys already sent
        seen_keys_set = set(seen_obs.keys())

        # Build new obs list and collect ALL new composite keys for every variable
        new_grouped_obs = []
        for obs_str in grouped_obs:
            # obs_str = "STID|dattim|{json_blob}"
            parts  = obs_str.split('|', 2)
            if len(parts) < 3:
                continue
            stid, dattim, blob = parts

            try:
                var_dict = json.loads(blob)
            except json.JSONDecodeError:
                continue

            # Collect ALL new composite keys for this obs_str — one per variable
            obs_is_new = False
            for vargem in var_dict:
                composite_key = f"{stid}|{dattim}|{vargem}"
                if composite_key not in seen_keys_set:
                    obs_is_new = True
                    new_obs_keys[composite_key] = send_time_iso

            # Include this obs_str if at least one variable is new
            if obs_is_new:
                new_grouped_obs.append(obs_str)

        logger.info(
            f"DEDUP: {len(new_grouped_obs)} new obs of {len(grouped_obs)} total "
            f"({len(grouped_obs) - len(new_grouped_obs)} already seen)"
        )

        # Send new observations to POE in chunks
        for chunk in poe.chunk_list(new_grouped_obs, chunk_size=int(args.poe_chunk_size)):
            io, _ = poe.poe_formatter(chunk, list(seen_keys_set))
            if io is None:
                logger.debug("POE: formatted io is empty for this chunk, skipping")
                continue
            if args.local_run:
                logger.debug("POE: local run — not sending to POE")
                logger.debug(io)
            else:
                poe.poe_insertion(io, args)
                time.sleep(2)

        # Run poe_formatter again on the full grouped_obs list to get the authoritative
        # seen_obs from POE's perspective — do NOT send this to POE, just use the
        # returned seen_obs list to drive the write-back and pruning steps below.
        io, seen_obs_list = poe.poe_formatter(grouped_obs, list(seen_keys_set))
        logger.debug(io)

        # Prune any entries older than the archive window using POE's own formatter
        data_archive_time = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)
        seen_obs_list = poe.seen_obs_formatter(seen_obs_list, data_archive_time)

        ####################################################################
        # PERSIST SEEN-OBS + UPLOAD
        ####################################################################
        if not args.local_run:
            # Merge new keys into the existing seen set and persist
            merged_seen = {**seen_obs, **new_obs_keys}
            save_seen_obs(seen_obs_file, merged_seen)
            aws.S3.upload_file(
                local_file_path=seen_obs_file,
                bucket=s3_bucket_name,
                s3_key=s3_seen_obs_file,
            )
            logger.info(f"SEEN-OBS: persisted {len(merged_seen)} entries to S3")

        success_flag = 1
 
    except Exception as e:
        logger.exception(e)
 
    finally:
        total_runtime = time.time() - start_runtime
        logger.info(json.dumps({'completion': success_flag, 'time': round(total_runtime, 2)}))
 
        # Overwrite the same S3 object each run in prod
        if not (args.local_run or args.dev) and log_file and s3_bucket_name:
            try:
                s3_log_key = posixpath.join(s3_work_dir, f"{INGEST_NAME}_obs.log")
                aws.S3.upload_file(local_file_path=log_file, bucket=s3_bucket_name, s3_key=s3_log_key)
            except Exception as e:
                logger.warning(f"Failed to upload run log to S3: {e}")
 
        logging.shutdown()
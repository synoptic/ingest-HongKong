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
# Column name mappings
# Each label maps to the exact column header(s) that carry its value(s).
# For wind the CSV has both speed and gust in one file; we key by label so
# each label picks the right column.
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

# HKO date-time column header
DATETIME_COL = "Date time"
STATION_COL  = "Automatic Weather Station"


def _station_name_to_stid(station_name: str) -> str:
    """
    Convert a human-readable HKO station name to a compact alphanumeric ID.
    Removes non-alphanumeric characters and uppercases the result, then
    prepends the network prefix "HKI".
    """
    import re
    cleaned = re.sub(r"[^A-Za-z0-9]", "", station_name).upper()
    return f"HKI{cleaned}"


def load_seen_obs(filepath):
    """Load already-processed observation keys from seen_obs.txt."""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_seen_obs(filepath, seen_set):
    """Write the full set of seen observation keys back to seen_obs.txt."""
    with open(filepath, 'w') as f:
        for key in sorted(seen_set):
            f.write(key + '\n')


########################################################################################################################
# FETCH
########################################################################################################################
def fetch_hko_data():
    """Fetch data from all HKO API endpoints."""
    incoming_data = {}
 
    for label, url in HKO_API_ENDPOINTS.items():
        try:
            logger.debug(f"FETCH: Requesting {label} -> {url}")
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            incoming_data[label] = resp.text
            logger.debug(f"FETCH: OK for {label} ({len(resp.text)} chars)")
 
        except Exception as e:
            # Log and continue — a single endpoint failure should not abort the whole run
            logger.warning(f"FETCH: Failed for {label}: {e}")
 
    if not incoming_data:
        logger.error("FETCH: All HKO endpoint requests failed")
        return None
 
    return incoming_data
 
 
########################################################################################################################
# DEFINE ETL/PARSING FUNCTIONS
########################################################################################################################
def _csv_to_structured(label: str, raw_csv: str) -> dict:
    """Convert raw CSV to structured dict."""
    import csv
    from io import StringIO
    lines = raw_csv.splitlines()
 
    # Extract comment/metadata lines that appear before the Year/Month/Day header
    header_idx = next(
        (i for i, l in enumerate(lines) if "Year" in l), None
    )
    meta_lines = lines[:header_idx] if header_idx is not None else []
 
    records = []
    if header_idx is not None:
        reader = csv.DictReader(StringIO('\n'.join(lines[header_idx:])))
        for row in reader:
            # Normalize keys — strip whitespace
            cleaned = {k.strip(): (v.strip() if v is not None else "") for k, v in row.items() if k}
            records.append(cleaned)
 
    return {
        "label": label,
        "meta": meta_lines,
        "data": {
            "records": records,
            "record_count": len(records),
        },
    }
 

def cache_raw_data_simple(incoming_data, work_dir: str, s3_bucket_name: str, s3_prefix: str):
    """Cache raw data to S3."""
    try:
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
        local_file_path = os.path.join(work_dir, f"{timestamp}.json")
 
        logger.debug(f"CACHE: target s3://{s3_bucket_name}/{s3_key}")
 
        # Save data to local file
        try:
            with open(local_file_path, 'w', encoding='utf-8') as f:
                json.dump(incoming_data, f, indent=2, ensure_ascii=False)
 
            file_size = os.path.getsize(local_file_path)
            logger.debug(f"CACHE: created local file {local_file_path} ({file_size} bytes)")
 
        except Exception as e:
            logger.error(f"CACHE: failed to create local file: {e}")
            return False
 
        # Upload to S3
        t1 = time.time()
        try:
            logger.debug("CACHE: uploading to S3")
            aws.S3.upload_file(local_file_path, s3_bucket_name, s3_key)
            logger.debug(f"CACHE: upload OK in {time.time()-t1:.2f}s; size={file_size}B")
            return True
 
        except Exception as e:
            logger.error(f"CACHE: failed to upload in {time.time()-t1:.2f}s: {e}")
            return False
 
    except Exception as e:
        logger.error(f"CACHE: unexpected error: {e}")
        return False
 
 
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
            tzinfo=hkt
        )
        return dt_hkt.astimezone(timezone.utc)
    except ValueError:
        return None


def _parse_value(label: str, raw_val: str) -> Optional[float]:
    """
    Extract a numeric value from a raw string and apply unit conversions
    using ingestlib (parse.get_translated_value / parse.create_conversion /
    parse.convert_units), driven entirely by the variables data dictionary.

    Returns the converted float, or None if the value is missing / non-numeric.
    """
    # Strip surrounding whitespace and quotes
    raw_val = raw_val.strip().strip('"')

    # Visibility comes as "17 km" — extract the numeric part only
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
            # Fall back to unconverted value if ingestlib can't resolve the conversion
            value = round(value, 3)
    else:
        value = round(value, 3)

    return value


def parse_hko_data(incoming_data, station_meta):
    """
    Parse the raw CSV data into a dict keyed by  station|dattim.

    HKO CSVs have this actual format (no Year/Month/Day columns):
        Date time,Automatic Weather Station,<value column(s)>
        202603311600,King's Park,27.0
    """
    import csv
    from io import StringIO
 
    grouped_obs_set = {}
    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)
    skipped_old   = 0
    #skipped_noval ensures only valid numeric observations go forward.
    skipped_noval = 0

    for label, raw_csv in incoming_data.items():
        if not raw_csv:
            continue

        var_config = variables.get(label)
        if not var_config:
            logger.warning(f"PARSE: No variable mapping for label '{label}', skipping")
            continue

        vargem = parse.get_translated_value(label, variables=variables, field='vargem')
        if not vargem:
            logger.warning(f"PARSE: Could not resolve vargem for label '{label}', skipping")
            continue
        vnum = int(variables[label]['VNUM'])

        # Identify which CSV column holds this label's value
        value_col = LABEL_VALUE_COLUMN.get(label)
        if not value_col:
            logger.warning(f"PARSE: No value-column mapping for label '{label}', skipping")
            continue

        try:
            # Strip UTF-8 BOM if present
            clean_csv = raw_csv.lstrip('\ufeff')
            lines = clean_csv.splitlines()
            if not lines:
                logger.warning(f"PARSE: Empty CSV for {label}, skipping")
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
                    f"Found: {fieldnames}. Skipping."
                )
                continue

            rows_list = list(reader)
            logger.debug(f"PARSE [{label}]: {len(rows_list)} rows, value_col='{value_col}'")

            for row in rows_list:
                # --- datetime ---
                raw_dt = row.get(DATETIME_COL, "").strip()
                dt_utc = _parse_hko_datetime(raw_dt)
                if dt_utc is None:
                    logger.debug(f"PARSE [{label}]: unparseable datetime '{raw_dt}', skipping row")
                    continue

                if dt_utc < cutoff_dt:
                    skipped_old += 1
                    continue

                dattim_str = dt_utc.strftime("%Y%m%d%H%M")

                # --- station ID ---
                station_name = row.get(STATION_COL, "").strip()
                if not station_name:
                    continue
                synoptic_stid = _station_name_to_stid(station_name)

                # --- value ---
                raw_val = row.get(value_col, "")
                value = _parse_value(label, raw_val)
                if value is None:
                    skipped_noval += 1
                    continue

                # --- merge into grouped_obs_set ---
                key = f"{synoptic_stid}|{dattim_str}"
                if key in grouped_obs_set:
                    grouped_obs_set[key][vargem] = {vnum: value}
                else:
                    grouped_obs_set[key] = {vargem: {vnum: value}}
 
        except Exception as e:
            logger.warning(f"PARSE: Failed to parse {label}: {e}")
            continue

    if skipped_old:
        logger.debug(
            f"PARSE: Skipped {skipped_old} rows older than "
            f"{cutoff_dt.strftime('%Y-%m-%dT%H:%MZ')}"
        )
    if skipped_noval:
        logger.debug(f"PARSE: Skipped {skipped_noval} rows with missing/non-numeric values")

    logger.debug(f"PARSE: Created {len(grouped_obs_set)} grouped observations")
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
 
    try:
        logger.debug("BOOT: ECS logging path OK")
 
        # paths
        os.makedirs(work_dir, exist_ok=True)
        s3_bucket_name = os.environ["INTERNAL_BUCKET_NAME"]
        cache_s3_bucket_name = os.environ.get("CACHE_S3_BUCKET_NAME", "synoptic-ingest-provider-data-cache-a4fb6")
 
        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")
        s3_seen_obs_file   = posixpath.join(s3_work_dir, "seen_obs.txt")
        seen_obs_file      = os.path.join(work_dir, "seen_obs.txt")
        station_meta_file  = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")
        grouped_obs_file   = os.path.join(work_dir, "grouped_obs.txt")
        # Determine the time before which data will not be archived
        data_archive_time = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)
 
        # Download seen observations file
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_seen_obs_file, local_directory=work_dir)
        except Exception as e:
            logger.warning(f"Warning: Failed to download {s3_seen_obs_file}. Error: {e}")
 
        # Download station metadata file
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_station_meta_file, local_directory=work_dir)
        except Exception as e:
            logger.warning(f"Warning: Failed to download {s3_station_meta_file}. Error: {e}")
 
        ########################################################################################################################
        # GET LATEST OBS
        ########################################################################################################################
 
        # load station metadata file
        if os.path.exists(station_meta_file):
            station_meta = parse.load_json_file(file_path=station_meta_file)
        else:
            station_meta = {}
 
        logger.debug("FETCH: starting data fetch request")
        # Fetch all HKO endpoints; returns dict { label: raw_csv } or None
        incoming_data = fetch_hko_data()
        logger.debug(f"FETCH: got data? {bool(incoming_data)}")
 
        # Convert raw CSV strings to structured dicts
        cache_data = None
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
 
        if incoming_data:
            logger.info(msg=json.dumps({'Incoming_Data_Success': 1}))

            # Parse incoming data into a grouped dict (station|dattim)
            grouped_obs_set = parse_hko_data(incoming_data, station_meta)

            if not grouped_obs_set:
                logger.warning("PARSE: No grouped observations generated, skipping validation and POE")
                return
            grouped_obs = [
                f"{k}|{json.dumps(v).replace(' ', '')}"
                for k, v in grouped_obs_set.items()
            ]

            ########################################################################################################################
            # VALIDATE DATA
            ########################################################################################################################
            # save the grouped obs and station meta if it exists
            if args.local_run or args.dev:
                dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
                os.makedirs(dev_dir, exist_ok=True)
 
                # Save grouped_obs as a text file (one line per observation string)
                grouped_obs_path = os.path.join(dev_dir, 'grouped_obs.txt')
                with open(grouped_obs_path, 'w') as f:
                    for obs in sorted(grouped_obs):
                        f.write(obs + '\n')
                logger.debug(f"[DEV] Saved grouped_obs to {grouped_obs_path}")
 
                # Save station_meta if available
                if 'station_meta' in locals():
                    station_meta_path = os.path.join(dev_dir, 'station_meta.json')
                    with open(station_meta_path, 'w') as f:
                        json.dump(station_meta, f, indent=4)
                    logger.debug(f"[DEV] Saved station_meta to {station_meta_path}")
 
            # Try to fetch variables_table unless local_run
            variables_table = {}
            if not args.local_run:
                try:
                    variables_table = metamgr.grab_variables_table(
                        socket_address=args.metamgr_socket_address,
                        socket_port=args.metamgr_socket_port
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
                variable_validators.append(partial(validator.validate_variables, variables_table=variables_table))
                variable_validators[-1].__name__ = "validate_variables"
            end_time   = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=24)
 
            # Observation validations
            obs_validators = [
                lambda obs: validator.validate_dattim(obs, start_time, end_time)
            ]
            if variables_table:
                obs_validators.append(partial(validator.validate_observation_ranges, variables_table=variables_table))
                obs_validators[-1].__name__ = "validate_observation_ranges"
 
            # Run validations
            all_validation_messages = []
            for vfunc in variable_validators:
                for m in vfunc(variables):
                    all_validation_messages.append((vfunc.__name__, m))  # tag with func name
            for ofunc in obs_validators:
                for m in ofunc(grouped_obs):
                    all_validation_messages.append((ofunc.__name__, m))
 
            if all_validation_messages:
                grouped = defaultdict(list)
                for name, msg in all_validation_messages:
                    grouped[name].append(msg)
                for func_name, msgs in grouped.items():
                    logger.debug(f"[{func_name}] {len(msgs)} occurrences")
                    for m in msgs:
                        logger.debug(f"[{func_name}] {m}")
            else:
                logger.debug(":: PASSED :: All variable and observation validations clean.")
 
            ########################################################################################################################
            # DIFF AGAINST SEEN OBS AND SEND TO POE
            ########################################################################################################################

            # Load already-processed keys (station|dattim).
            # Empty set on first run → full backfill.
            seen_obs = load_seen_obs(seen_obs_file)
            new_obs_keys = set()

            # Filter to only observations not yet sent to POE
            filtered_obs = []
            for obs_str in grouped_obs:
                # obs_str format: "station|dattim|{json_blob}"
                # The dedup key is the first two pipe-delimited parts.
                key = '|'.join(obs_str.split('|')[:2])
                if key not in seen_obs:
                    filtered_obs.append(obs_str)
                    new_obs_keys.add(key)

            logger.debug(f"DEDUP: {len(filtered_obs)} new obs of {len(grouped_obs)} total")

            for chunk in poe.chunk_list(filtered_obs, chunk_size=int(args.poe_chunk_size)):
                io, _ = poe.poe_formatter(chunk, [])
                if io is None:
                    logger.debug("io is empty")
                elif args.local_run:
                    logger.debug("Local Run, therefore NOT sending to any POE")
                else:
                    poe.poe_insertion(io, args)
                    time.sleep(2)

            io, _ = poe.poe_formatter(filtered_obs, [])
            logger.debug(io)

            ########################################################################################################################
            # UPLOAD TO S3
            ########################################################################################################################
            if not args.local_run:
                # Merge new keys into the existing seen set and persist
                save_seen_obs(seen_obs_file, seen_obs | new_obs_keys)
                aws.S3.upload_file(local_file_path=seen_obs_file, bucket=s3_bucket_name, s3_key=s3_seen_obs_file)
 
            success_flag = 1
 
        else:
            logger.error(msg=json.dumps({'Incoming_Data_Success': 0}))
 
    except Exception as e:
        logger.exception(e)
 
    finally:
        total_runtime = time.time() - start_runtime
        logger.info(msg=json.dumps({'completion': success_flag, 'time': total_runtime}))
 
        # Overwrite the same S3 object each run in prod
        if not (args.local_run or args.dev) and log_file:
            try:
                s3_log_key = posixpath.join(s3_work_dir, f"{INGEST_NAME}_obs.log")
                aws.S3.upload_file(local_file_path=log_file, bucket=s3_bucket_name, s3_key=s3_log_key)
            except Exception as e:
                logger.warning(f"Failed to upload run log to S3: {e}")
 
        logging.shutdown()
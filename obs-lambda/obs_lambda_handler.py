import logging
from datetime import datetime, timezone, timedelta
from ingestlib import poe, parse, aws, validator, metamgr,  core
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
 
# HKO API base URL and endpoints for each variable dataset.
# Each key is a human-readable label; value is the full API URL.
# All endpoints return JSON. Update URLs as needed.
# Ref: https://data.gov.hk/en-data/dataset/hk-hko-rss-*
_BASE = "https://data.weather.gov.hk/weatherAPI/cis/csvfile"
_WIND = "https://data.weather.gov.hk/weatherAPI/cis/csvfile"
 
HKO_API_ENDPOINTS = {
    # HKO station — confirmed working
    "grass_min_temp":    f"{_BASE}/HKO/ALL/daily_HKO_GMT_ALL.csv",
    "mean_cloud":        f"{_BASE}/HKO/ALL/daily_HKO_CLD_ALL.csv",
    "total_rainfall":    f"{_BASE}/HKO/ALL/daily_HKO_RF_ALL.csv",
    "mean_humidity":     f"{_BASE}/HKO/ALL/daily_HKO_RH_ALL.csv",
    "max_mean_min_temp": f"{_BASE}/HKO/ALL/daily_HKO_TEMP_ALL.csv",
    "dew_point_temp":    f"{_BASE}/HKO/ALL/daily_HKO_DEW_ALL.csv",
    "wet_bulb_temp":     f"{_BASE}/HKO/ALL/daily_HKO_WET_ALL.csv",
    "mean_pressure":     f"{_BASE}/HKO/ALL/daily_HKO_MSLP_ALL.csv",
    # KP station
    "heat_index":        f"{_BASE}/KP/ALL/daily_KP_MEANHKHI_ALL.csv",
    "total_evaporation": f"{_BASE}/KP/ALL/daily_KP_EVAP_ALL.csv",
    "solar_radiation":   f"{_BASE}/KP/ALL/daily_KP_GSR_ALL.csv",
    "uv_index":          f"{_BASE}/KP/ALL/daily_KP_UV_ALL.csv",
    "sunshine_hours":    f"{_BASE}/KP/ALL/daily_KP_SUN_ALL.csv",
    "mean_wind_speed":   f"{_BASE}/KP/ALL/daily_KP_WSPD_ALL.csv",
    "wind_direction":    f"{_BASE}/KP/ALL/daily_KP_WDIR_ALL.csv",
    # WGL station
    "mean_sea_temp":     f"{_BASE}/WGL/ALL/daily_WGL_SST_ALL.csv",
    # HKA station
    "visibility_hours":  f"{_BASE}/HKA/ALL/daily_HKA_RVIS_ALL.csv",
}
 
LABEL_TO_STATION = {
    label: url.split('/')[-3]
    for label, url in HKO_API_ENDPOINTS.items()
}


def load_seen_obs(filepath):
    """Load already-processed observation keys from seen_obs.txt.
    Keys are station|dattim — one per line.
    Returns empty set on first run (file empty or missing).
    """
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
    """
    Fetch data from all HKO API endpoints defined in HKO_API_ENDPOINTS.
 
    Mirrors the meteoswiss fetch pattern: iterate over known endpoints,
    request each one, and return a dict keyed by variable label.
 
    Returns:
        dict: { variable_label: raw_text_content } for successful fetches,
              or None if all requests fail.
    """
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
    """
    Convert a raw HKO CSV string into a structured dict matching the
    Singapore-style cache format, so the cached JSON contains parsed
    records rather than raw CSV text.
    """
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
    """
    Save raw incoming data dict to a timestamped JSON file and upload to S3.
    Each run creates a unique file — no diffing or merging.
    """
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
def parse_hko_data(incoming_data, station_meta):
    import csv
    from io import StringIO
 
    grouped_obs_set = {}
 
    for label, raw_csv in incoming_data.items():
        if not raw_csv:
            continue
 
        station_id = LABEL_TO_STATION.get(label, "")
        synoptic_stid = station_meta.get(station_id, {}).get("SYNOPTIC_STID", station_id)
 
        # Key in data_dictionary IS the endpoint label — direct lookup
        var_config = variables.get(label)
        if not var_config:
            logger.warning(f"PARSE: No variable mapping for label '{label}', skipping")
            continue
 
        vargem = var_config["vargem"]
        vnum   = var_config["VNUM"]
 
        try:
            lines = raw_csv.splitlines()
            header_idx = next((i for i, l in enumerate(lines) if "Year" in l), None)
            if header_idx is None:
                logger.warning(f"PARSE: Could not find header row for {label}, skipping")
                continue
 
            reader = csv.DictReader(StringIO('\n'.join(lines[header_idx:])))
 
            for row in reader:
                try:
                    year  = next((row[k].strip() for k in row if "Year"  in k), "")
                    month = next((row[k].strip() for k in row if "Month" in k), "")
                    day   = next((row[k].strip() for k in row if "Day"   in k), "")
 
                    if not (year and month and day):
                        continue
 
                    dt_utc = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
                    dattim_str = dt_utc.strftime("%Y%m%d%H%M")
 
                except (ValueError, AttributeError):
                    continue
 
                raw_val = next((row[k].strip() for k in row if "Value" in k), "")
                if not raw_val:
                    continue
 
                try:
                    value = round(float(raw_val), 3)
                except ValueError:
                    continue
 
                if vargem in ["PMSL", "PRES"]:
                    value = value * 100

                # Key is station|dattim — multiple variables for the same
                # station+timestamp are merged into a single obs record.
                key = f"{synoptic_stid}|{dattim_str}"
                if key in grouped_obs_set:
                    grouped_obs_set[key][vargem] = {vnum: value}
                else:
                    grouped_obs_set[key] = {vargem: {vnum: value}}
 
        except Exception as e:
            logger.warning(f"PARSE: Failed to parse {label}: {e}")
            continue
 
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
 
    # --- logging (stdout + single file; overwrites each run) ---
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
        s3_seen_obs_file     = posixpath.join(s3_work_dir, "seen_obs.txt")
        seen_obs_file        = os.path.join(work_dir, "seen_obs.txt")
        station_meta_file    = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")
 
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
 
        # Convert raw CSV strings to structured dicts before caching, so the
        # cached JSON matches the Singapore-style format (parsed records, not raw text).
        # parse_hko_data still receives the original raw CSV strings for parsing.
        cache_data = None
        if incoming_data:
            cache_data = {
                label: _csv_to_structured(label, raw_csv)
                for label, raw_csv in incoming_data.items()
            }
 
        # store raw incoming data in the data provider raw cache bucket
        cache_raw_data_simple(
            incoming_data=cache_data,
            work_dir=work_dir,
            s3_bucket_name=cache_s3_bucket_name,
            s3_prefix=INGEST_NAME
        )
 
        if incoming_data:
            logger.info(msg=json.dumps({'Incoming_Data_Success': 1}))

            # Parse all fetched CSVs into a unified grouped observations dict.
            # Keys from parse_hko_data are station|dattim.
            grouped_obs_set = parse_hko_data(incoming_data, station_meta)

            # Build obs strings: "station|dattim|{json_blob}"
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
 
            # Define the valid dattim window for observation validation.
            # HKO serves historical daily data, so we accept everything from
            # a generous past horizon up to the current moment.
            LOOKBACK_DAYS = 365 * 200   # ~200 years covers the full HKO archive
            end_time   = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=LOOKBACK_DAYS)
 
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
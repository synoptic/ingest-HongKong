import csv
import requests
from io import StringIO
from datetime import datetime, timedelta
from ingestlib import aws, station_lookup, parse, core
import sys, os
import ssl
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from enum import IntEnum
import boto3
import json
import logging
import time
import math
import posixpath
import re, unicodedata
from bs4 import BeautifulSoup

########################################################################################################################
# OVERVIEW
########################################################################################################################
# Hong Kong station metadata ingest.
#   1. The station metadata JSON MUST be downloaded from S3 at the start of every run.
#      Defaulting existing_stations to {} would mask S3 load failures and risk
#      overwriting live station records — this is never permitted.
#   2. Fetched stations are merged INTO existing_stations:
#        - SYNOPTIC_STID is immutable once assigned.
#        - Mutable fields (NAME / LAT / LON / OTID / ELEVATION) are updated from scrape.
#        - Stations absent from today's scrape are KEPT (not silently deleted).
#   3. station_lookup.load_metamgr is called with the merged payload.
#   4. The merged station_meta is persisted back to S3 along with generated SQL files.
########################################################################################################################

########################################################################################################################
# DEFINE CONSTANTS
########################################################################################################################
INGEST_NAME = "hongkong" #TODO Update Ingest Name
M_TO_FEET = 3.28084
ELEVATION_UNIT = 'METERS' # ELEVATION UNIT OF THIS INGESTS METADATA MUST BE EITHER 'METERS' OR 'FEET'. METAMOTH CURRENTLY STORES ELEVATION IN FEET, SO WE WILL CONVERT IF IT'S IN METERS. 
MNET_ID = 342 # CREATE NEW MNET_ID FOR THIS INGEST
MNET_SHORTNAME = "hongkong" #TODO add the mnet shortname
RESTRICTED_DATA_STATUS = False # True or False, IS THE DATA RESTRICTED?
RESTRICTED_METADATA_STATUS = False # True or False, IS THE METADATA RESTRICTED?
STID_PREFIX = "HKI" #TODO add the stid prefix

HKO_STATION_URL = "https://www.hko.gov.hk/en/cis/stn.htm"

# HTTP retry config for metadata scrape
_META_MAX_RETRIES   = 3
_META_BACKOFF       = 2.0   # seconds; wait = backoff * attempt

########################################################################################################################
# DEFINE LOGS
########################################################################################################################
logger = logging.getLogger(f"{INGEST_NAME}_ingest")


########################################################################################################################
# FETCH STATION METADATA
########################################################################################################################

def _dms_to_decimal(dms_str: str):
    """
    Convert a DMS (degrees / minutes / seconds) coordinate string to decimal degrees.

    Handles formats such as:
        22°18'07"N    22°18'N    22°18.1'N
    Returns a rounded float or None if the string cannot be parsed.
    """
    try:
        dms_str = dms_str.strip()
        match = re.match(
            r"(\d+)[°d]\s*(\d+(?:\.\d+)?)?['′]?\s*(\d+(?:\.\d+)?)?[\"″]?\s*([NSEW])?",
            dms_str
        )
        if not match:
            return None

        degrees   = float(match.group(1))
        minutes   = float(match.group(2)) if match.group(2) else 0.0
        seconds   = float(match.group(3)) if match.group(3) else 0.0
        direction = match.group(4)

        decimal = degrees + minutes / 60.0 + seconds / 3600.0
        if direction in ('S', 'W'):
            decimal *= -1

        return round(decimal, 6)
    except Exception:
        return None


def fetch_hko_station_metadata_raw() -> dict:
    """
    Scrape the HKO station list page and return a dict keyed by station code.

    Retries up to _META_MAX_RETRIES times with exponential back-off.

    Each entry has the shape::

        {
            "NAME":      str,
            "LAT":       float,
            "LON":       float,
            "OTID":      str,   # == station code
            "ELEVATION": float | None,
        }

    Stations with unparseable coordinates are skipped and counted so the
    operator can see how many were lost.
    """
    out: dict = {}
    skipped = 0

    for attempt in range(1, _META_MAX_RETRIES + 1):
        try:
            resp = requests.get(HKO_STATION_URL, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            wait = _META_BACKOFF * attempt
            logger.warning(
                f"META FETCH: attempt {attempt}/{_META_MAX_RETRIES} failed — {e}; "
                f"retrying in {wait:.1f}s"
            )
            if attempt < _META_MAX_RETRIES:
                time.sleep(wait)
            else:
                logger.error(f"META FETCH: all attempts exhausted for {HKO_STATION_URL}")
                return out

    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")

        if len(rows) < 2:
            continue

        for row in rows[1:]:
            cols = row.find_all("td")

            if len(cols) < 4:
                continue

            try:
                name_cell = cols[0].get_text(strip=True)

                # Station code is inside parentheses, e.g. "King's Park (KP)"
                match = re.search(r"\((.*?)\)", name_cell)
                if not match:
                    continue

                code = match.group(1).strip()
                name = name_cell.split("(")[0].strip()

                lat_raw = cols[1].get_text(strip=True)
                lon_raw = cols[2].get_text(strip=True)
                elev_raw = cols[3].get_text(strip=True)

                lat = _dms_to_decimal(lat_raw)
                lon = _dms_to_decimal(lon_raw)

                if lat is None or lon is None:
                    logger.debug(
                        f"META FETCH: skipping '{name}' ({code}) — "
                        f"could not parse lat='{lat_raw}' lon='{lon_raw}'"
                    )
                    skipped += 1
                    continue

                elevation: float | None = None
                try:
                    elevation = float(elev_raw)
                except (ValueError, TypeError):
                    pass

                out[code] = {
                    "NAME": name,
                    "LAT": lat,
                    "LON": lon,
                    "OTID": code,
                    "ELEVATION": elevation,
                }

            except Exception as e:
                logger.debug(f"META FETCH: skipping row due to error: {e}")
                skipped += 1
                continue

    logger.info(
        f"META FETCH: scraped {len(out)} stations "
        f"({skipped} rows skipped due to parse errors)"
    )
    return out


########################################################################################################################
# PAYLOAD GENERATION
########################################################################################################################
def generate_metadata_payload(station_meta: dict, payload_type: str, source_info: dict = None):
    """
    Generate the metadata payload for ingestlib station_lookup or metamanager.

    Args:
        station_meta:  Merged station dict (SYNOPTIC_STID already resolved).
        payload_type:  ``'station_lookup'`` or ``'metamanager'``.
        source_info:   Optional override for the metamanager source block.

    Returns:
        dict  for ``station_lookup`` payloads.
        str   (JSON) for ``metamanager`` payloads.
    """
    if payload_type not in {"station_lookup", "metamanager"}:
        raise ValueError("payload_type must be 'station_lookup' or 'metamanager'")

    metadata = []
    skipped_missing  = 0
    skipped_invalid  = 0

    for station_id, row in station_meta.items():
        try:
            # Extract required fields from the row
            stid = row.get('SYNOPTIC_STID', None)
            name = row.get('NAME', None)
            lat = row.get('LAT', None)
            lon = row.get('LON', None)
            otid = row.get('OTID', None)
            elevation = row.get('ELEVATION', None)


            # Name sanitisation
            if name:
                clean_name = core.ascii_sanitize(name) if not name.isascii() else name
            else:
                clean_name = None

            # Validate required fields before attempting type conversion
            if not stid or not clean_name:
                logger.debug(
                    f"PAYLOAD: skipping {station_id} — missing STID or NAME"
                )
                skipped_missing += 1
                continue

            if lat is None or lon is None:
                logger.debug(f"PAYLOAD: skipping {station_id} — missing LAT/LON")
                skipped_missing += 1
                continue
            lat = float(lat)
            lon = float(lon)
            if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
                logger.debug(f"PAYLOAD: skipping {station_id} — invalid lat/lon ({lat}, {lon})")
                skipped_invalid += 1
                continue

            # Check Elevation
            if elevation is not None:
                elevation = float(elevation)
                
                if ELEVATION_UNIT == 'METERS':
                    elevation *= M_TO_FEET
                elif ELEVATION_UNIT != 'FEET':
                    raise ValueError(f"Invalid ELEVATION_UNIT: '{ELEVATION_UNIT}'")
                
                if math.isnan(elevation):
                    elevation = None

            metadata.append({
                    "STID": stid,
                    "NAME": clean_name,
                    "LATITUDE": lat,
                    "LONGITUDE": lon,
                    "OTHER_ID": otid,
                    "MNET_ID": MNET_ID,
                    "ELEVATION": None if elevation is None else round(elevation, 3),
                    "RESTRICTED_DATA": row.get('RESTRICTED_DATA', RESTRICTED_DATA_STATUS),
                    "RESTRICTED_METADATA": row.get('RESTRICTED_METADATA', RESTRICTED_METADATA_STATUS)
            })
        except ValueError as exc:
            logger.debug(f"PAYLOAD: skipping {station_id} — {exc}")
            skipped_invalid += 1

    logger.info(
        f"PAYLOAD: built {len(metadata)} station records "
        f"(skipped: missing={skipped_missing}, invalid={skipped_invalid})"
    )
    
    if payload_type == "station_lookup":
        return {"MNET_ID": MNET_ID, "STNS": metadata}

    # metamanager
    default_source = {"name": "Administration Console", "environment": str(MNET_ID)}
    payload = {
        "source":   source_info if source_info else default_source,
        "metadata": metadata,
    }
    return json.dumps(payload, indent=4)



def update_stations(url: str, headers: dict, payload: str) -> requests.Response:
    """Send a PUT request to update station data."""
    return requests.request("PUT", url, headers=headers, data=payload)


def save_to_json(data, filename: str) -> None:
    """Write *data* as indented JSON to *filename*, creating parent directories."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

########################################################################################################################
# MAIN FUNCTION
########################################################################################################################
def main(event,context):
    from args import args

    # ----- choose dirs once -----
    if args.local_run or args.dev:
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
        work_dir = "../dev/"
        s3_work_dir = "metadata/"
    else:
        log_dir = "/tmp/tmp/"
        work_dir = "/tmp/tmp/"
        s3_work_dir = "metadata/"

    # ----- logging (stdout + one file) -----
    log_file = core.setup_logging(
        logger, INGEST_NAME,
        log_level=getattr(args, "log_level", "INFO"),
        write_logs=True,
        log_dir=log_dir,
        filename=f"{INGEST_NAME}_meta.log"
    )

    core.setup_signal_handler(logger, args)

    logger.debug(f"ARGS LOADED from: {__file__}")
    logger.debug(
        f"ENV at load time: DEV={os.getenv('DEV')} "
        f"LOCAL_RUN={os.getenv('LOCAL_RUN')} "
        f"LOG_LEVEL={os.getenv('LOG_LEVEL')}"
    )
    logger.debug(vars(args))
    
    start_runtime = time.time()
    s3_bucket_name = None   # defined early so finally block can reference it safely

    try:
        # Declare S3 Paths for Metadata Storage
        s3_bucket_name = os.environ.get('INTERNAL_BUCKET_NAME')
        if not s3_bucket_name:
            raise ValueError("Missing required env var: INTERNAL_BUCKET_NAME")

        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # Declare Local Paths
        os.makedirs(work_dir, exist_ok=True)
        station_meta_file = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # load existing stations from S3 
        # We must never default to {} here.  If the download fails we raise
        # immediately — proceeding with an empty dict would silently overwrite
        # all previously-assigned SYNOPTIC_STIDs.
        existing_stations: dict = {}
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_station_meta_file, local_directory=work_dir)
            with open(station_meta_file, 'r', encoding='utf-8') as json_file:
                existing_stations = json.load(json_file)
            logger.info(f"STATION META: loaded {len(existing_stations)} existing stations from S3")
        except FileNotFoundError:
            logger.info("STATION META: no existing file found in S3 — first run assumed")
        except Exception as e:
            # Any other failure (network, malformed JSON, …) is a hard error
            logger.error(f"STATION META: failed to load from S3: {e}")
            raise RuntimeError(
                f"Cannot proceed: failed to load existing station metadata: {e}"
            ) from e

        ########################################################################################################################
        # Fetch Metadata
        ########################################################################################################################        raw_data = fetch_hko_station_metadata_raw()
        raw_data = fetch_hko_station_metadata_raw()

        if not raw_data:
            logger.warning(
                "META FETCH: returned 0 stations — "
                "skipping merge to avoid overwriting existing data"
            )
            # Do not raise; let the run complete so logs are uploaded
            return

        # ── MERGE scraped stations INTO existing_stations ─────────────────
        #
        # Rules:
        #   • SYNOPTIC_STID is immutable — never overwrite if already set.
        #   • Mutable fields (NAME, LAT, LON, OTID, ELEVATION) are updated
        #     from the fresh scrape.
        #   • Stations absent from today's scrape are retained as-is.
        #   • Restriction flags default from constants but are preserved if
        #     already set in existing_stations.
        new_count     = 0
        updated_count = 0

        station_meta: dict = dict(existing_stations)  # start from the full existing set

        for code, info in raw_data.items():
            existing = existing_stations.get(code, {})
            is_new   = code not in existing_stations

            # Assign SYNOPTIC_STID only on first appearance
            synoptic_stid = existing.get("SYNOPTIC_STID") or f"{STID_PREFIX}{code}"

            station_meta[code] = {
                "SYNOPTIC_STID": synoptic_stid,
                "NAME": info["NAME"],
                "LAT": info["LAT"],
                "LON": info["LON"],
                "OTID": info["OTID"],
                "ELEVATION": info.get("ELEVATION"),
                # Preserve any existing restriction overrides; fall back to constants
                "RESTRICTED_DATA":     existing.get("RESTRICTED_DATA",    RESTRICTED_DATA_STATUS),
                "RESTRICTED_METADATA": existing.get("RESTRICTED_METADATA", RESTRICTED_METADATA_STATUS),
            }

            if is_new:
                new_count += 1
            else:
                updated_count += 1

        retained_count = len(existing_stations) - (len(raw_data) - new_count)
        logger.info(
            f"MERGE: {new_count} new, {updated_count} updated, "
            f"{retained_count} retained (absent from today's scrape), "
            f"{len(station_meta)} total"
        )

        # ── GENERATE payload ──────────────────────────────────────────────
        station_lookup_payload = generate_metadata_payload(
            station_meta=station_meta,
            payload_type='station_lookup',
        )

        # ── DEV: persist locally and exit early ───────────────────────────
        if args.local_run:
            dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
            os.makedirs(dev_dir, exist_ok=True)

            meta_dev_path   = os.path.join(dev_dir, f'{INGEST_NAME}_stations_metadata.json')
            lookup_dev_path = os.path.join(dev_dir, f'{INGEST_NAME}_station_lookup_payload.json')
            with open(meta_dev_path, 'w') as fh:
                json.dump(station_meta, fh, indent=4)
            with open(lookup_dev_path, 'w') as fh:
                json.dump(station_lookup_payload, fh, indent=4)

            logger.debug(f"[DEV] station_meta → {meta_dev_path} ({len(station_meta)} stations)")
            logger.debug(f"[DEV] station_lookup_payload → {lookup_dev_path}")
            logger.info("DONE (local run — skipping station_lookup and S3 uploads)")
            return

        # ── PRODUCTION: station_lookup + S3 persist ───────────────────────
        logger.debug("PROD: calling station_lookup.load_metamgr")
        try:
            station_lookup.load_metamgr(station_lookup_payload, logstream=logger, mode='prod', output_location=work_dir)
            logger.debug("PROD: station_lookup.load_metamgr completed")
        except Exception as e:
            logger.exception(f"PROD: station_lookup failed: {e}")
            raise

        # --------------- 5. DATA PERSISTENCE ---------------

        # Save and upload station_meta
        save_to_json(data=station_meta, filename=station_meta_file)
        aws.S3.upload_file(
            local_file_path=station_meta_file,
            bucket=s3_bucket_name,
            s3_key=s3_station_meta_file
        )
        logger.info(f"PROD: station_meta uploaded to s3://{s3_bucket_name}/{s3_station_meta_file}")

        # Persist station_lookup_payload
        lookup_file = os.path.join(work_dir, f'{INGEST_NAME}_station_lookup_payload.json')
        save_to_json(data=station_lookup_payload, filename=lookup_file)
        s3_lookup_key = posixpath.join(s3_meta_work_dir,
                                       f'{INGEST_NAME}_station_lookup_payload.json')
        aws.S3.upload_file(
            local_file_path=lookup_file,
            bucket=s3_bucket_name,
            s3_key=s3_lookup_key,
        )

        # Upload generated SQL files
        deleted_sql = aws.S3.delete_files(
            bucket=s3_bucket_name,
            prefix=s3_meta_work_dir,
            endswith=".sql",
        )
        logger.debug(f"PROD: deleted {deleted_sql} old SQL files from S3")

        for file_name in os.listdir(work_dir):
            if not file_name.endswith(".sql"):
                continue
            sql_local  = os.path.join(work_dir, file_name)
            s3_sql_key = posixpath.join(
                os.path.dirname(s3_station_meta_file),
                os.path.basename(sql_local),
            )
            aws.S3.upload_file(
                local_file_path=sql_local,
                bucket=s3_bucket_name,
                s3_key=s3_sql_key,
            )
            logger.debug(f"PROD: uploaded SQL {file_name} → s3://{s3_bucket_name}/{s3_sql_key}")

        logger.info(msg=json.dumps({'completion': 1, 'time': round(time.time() - start_runtime, 2)}))

    except Exception as e:
        logger.error(msg=json.dumps({'completion': 0, 'time': round(time.time() - start_runtime, 2)}))
        raise 
        
    finally:
        total_runtime = time.time() - start_runtime
        logger.info(f"Total execution time: {total_runtime:.2f} seconds")

        # Upload log in prod only
        if not (args.local_run or args.dev) and log_file and s3_bucket_name:
            s3_log_key = posixpath.join(s3_work_dir, f"{INGEST_NAME}_meta.log")
            try:
                aws.S3.upload_file(
                    local_file_path=log_file,
                    bucket=s3_bucket_name,
                    s3_key=s3_log_key,
                )
            except Exception as e:
                logger.warning(f"LOG UPLOAD: failed — {e}")
        
        logging.shutdown()

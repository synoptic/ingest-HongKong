# ── Ingest identity ────────────────────────────────────────

NAME = "hongkong" # Matches existing codebase naming convention
DISPLAY_NAME = "HongKong" # For stack names, descriptions
MNET_ID = 342 # Unique MNET ID for HKO (sequential after Taiwan's 328)
STID_PREFIX = "HKI" # T= Taiwan region prefix pattern, HKO for Hong Kong Observatory
INCOMING_ELEVATION_UNIT = "METERS" # Standard for HKO AWS elevations

RESTRICTED_DATA = False
RESTRICTED_METADATA = False

LOOKUP_KEY = "STID" # Standard lookup key consistent with Taiwan

# ── Attribution ───────────────────────────────────────────

ATTRIBUTIONS = [
    {
        "owner": {"name": "Hong Kong Observatory", "ownertype_id": 10}, # Government meteorological agency
        "tier": 1,      # 1 = primary data owner
        "type_id": 10,  # 10 = meteorological agency
    },
]

# ── MNET (for generate-mnet SQL) ──────────────────────────

MNET_SHORTNAME = "HKI" # Standard abbreviation
MNET_LONGNAME  = "Hong Kong Observatory" # Full official name
MNET_URL       = "https://www.hko.gov.hk/en/index.html" # Official English site
MNET_OBTAIN    = "DIRECT"        # data obtained directly from the provider (not via broker)
MNET_ACQMETHOD = "API"           # HKO exposes a public REST/JSON API
MNET_CATEGORY  = 14              # 14 = surface synoptic / land-based met network

# ── AWS account & networking ──────────────────────────────
# Deployed in the same shared ingestion account as other Asia-Pacific sources.
# us-west-2 is the designated region for APAC ingests (proximity + latency trade-off).
ACCOUNT    = "905418025696"       # shared Synoptic ingest AWS account
REGION     = "us-west-2"         # primary ingest region for APAC
VPC_ID     = "vpc-09a07e46ba606169b"   # shared ingest VPC (same as Taiwan)
SUBNET_IDS = [
    "subnet-07287ffd7252971f0",   # us-west-2a — primary AZ
    "subnet-069ceb1375036964b",   # us-west-2b — failover AZ
]
# ── S3 buckets ────────────────────────────────────────────

INGEST_S3_BUCKET = "synoptic-ingest-hongkong-hko726" 
CACHE_S3_BUCKET = "synoptic-ingest-provider-data-cache-a4fb6" # Shared cache bucket

# ── Connection defaults ───────────────────────────────────
# These are injected into Lambda env vars by app.py at deploy time.
# POE (Point of Entry) — internal Synoptic data ingestion gateway.
POE_ADDRESS    = "mesonet-v2.entry.int.synopticdata.net"
POE_PORT       = "8095"
POE_CHUNK_SIZE = "2000"          # max observations per POST to POE; tune if HKO payloads are large

# MetaManager — internal service that persists station metadata.
METAMGR_ADDRESS = "10.14.159.245"
METAMGR_PORT    = "8888"

# ── Obs event source ─────────────────────────────────────
# "schedule" — CloudWatch cron, runs every OBS_SCHEDULE_MINUTES
# "queue"    — SQS, triggered by messages on OBS_QUEUE_URL
#
# HKO publishes hourly synoptic reports and 10-minute AWS (Automatic Weather Station)
# data. We poll every 10 minutes to capture the sub-hourly AWS updates promptly
# while staying within HKO's fair-use API rate limits.
OBS_EVENT_SOURCE     = "schedule"
OBS_SCHEDULE_MINUTES = 10  
MNET_PERIOD = OBS_SCHEDULE_MINUTES

# ── Meta schedule ─────────────────────────────────────────
# Station metadata changes infrequently (new stations, closures, coord corrections).
# A single daily refresh at midnight UTC is sufficient.
META_EVENT_SOURCE  = "schedule"
META_SCHEDULE_CRON = {"hour": "0", "minute": "0"}   # 00:00 UTC = 08:00 HKT

# ── Lambda sizing ─────────────────────────────────────────
# HKO has ~130 AWS stations — payload sizes are moderate.
OBS_COMPUTE      = "lambda"      # "lambda" or "fargate" 
OBS_MEMORY_MB    = 512           # 512 MB handles HKO JSON parsing + POE chunking comfortably
OBS_TIMEOUT_MIN  = 4             # must complete well within the 10-min schedule window
OBS_CONCURRENCY = 10             # Supports parallel requests to multiple HKO endpoints

META_COMPUTE     = "lambda"      # metadata volume is low; fargate not needed
META_MEMORY_MB   = 512           # station list + attribute mapping fits comfortably in 512 MB
META_TIMEOUT_MIN = 10            # allow extra time for full metadata enumeration and upserts
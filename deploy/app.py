#!/usr/bin/env python3
"""
CDK app — deploy obs and meta Lambda stacks.
All ingest-specific values come from config/__init__.py.
Only deployment-mode flags (MODE, LOG_LEVEL) are set here.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import aws_cdk as cdk
from stacks.obs_lambda_stack import ObsLambdaStack
from stacks.meta_lambda_stack import MetaLambdaStack
import config

env = {"account": config.ACCOUNT, "region": config.REGION}

ingest_arn = f"arn:aws:s3:::{config.INGEST_S3_BUCKET}"
cache_arn  = f"arn:aws:s3:::{config.CACHE_S3_BUCKET}"

stack_config = {
    "vpc_id":     config.VPC_ID,
    "subnet_ids": config.SUBNET_IDS,
    "s3_bucket_arns": [
        ingest_arn, f"{ingest_arn}/*",
        cache_arn,  f"{cache_arn}/*",
    ],
    "obs_docker_dir":  "../handlers",
    "meta_docker_dir": "../handlers",
    "obs_event_source":     getattr(config, "OBS_EVENT_SOURCE", "schedule"),
    "obs_schedule_minutes": getattr(config, "OBS_SCHEDULE_MINUTES", 5),
    "obs_queue_url":        getattr(config, "OBS_QUEUE_URL", None),
    "obs_queue_region":     getattr(config, "OBS_QUEUE_REGION", config.REGION),
    "obs_queue_batch":      getattr(config, "OBS_QUEUE_BATCH", 1),
    "obs_memory_mb":    getattr(config, "OBS_MEMORY_MB", 256),
    "obs_timeout_min":  getattr(config, "OBS_TIMEOUT_MIN", 5),
    "obs_concurrency":  getattr(config, "OBS_CONCURRENCY", 1),
    "meta_memory_mb":   getattr(config, "META_MEMORY_MB", 256),
    "meta_timeout_min": getattr(config, "META_TIMEOUT_MIN", 10),
    "meta_event_source":     getattr(config, "META_EVENT_SOURCE", "schedule"),
    "meta_schedule_cron":    getattr(config, "META_SCHEDULE_CRON", {"hour": "0", "minute": "0"}),
    "meta_queue_url":        getattr(config, "META_QUEUE_URL", None),
    "meta_queue_region":     getattr(config, "META_QUEUE_REGION", config.REGION),
    "meta_queue_batch":      getattr(config, "META_QUEUE_BATCH", 1),
}

obs_lambda_env = {
    "MODE":                 "prod",
    "LOG_LEVEL":            "INFO",
    "INGEST_NAME":          config.NAME,
    "INTERNAL_BUCKET_NAME": config.INGEST_S3_BUCKET,
    "CACHE_BUCKET_NAME":    config.CACHE_S3_BUCKET,
    "POE_SOCKET_ADDRESS":   config.POE_ADDRESS,
    "POE_SOCKET_PORT":      config.POE_PORT,
    "POE_CHUNK_SIZE":       config.POE_CHUNK_SIZE,
    "FORCE_IO_DUMP":        "true",
}

meta_lambda_env = {
    "MODE":                    "prod",
    "LOG_LEVEL":               "DEBUG",
    "INGEST_NAME":             config.NAME,
    "INTERNAL_BUCKET_NAME":    config.INGEST_S3_BUCKET,
    "METAMGR_SOCKET_ADDRESS":  config.METAMGR_ADDRESS,
    "METAMGR_SOCKET_PORT":     config.METAMGR_PORT,
    "FORCE_IO_DUMP":           "true",
}

_OBS_STACKS = {"lambda": ObsLambdaStack}
_META_STACKS = {"lambda": MetaLambdaStack}

display = config.DISPLAY_NAME
app = cdk.App()

ObsStack = _OBS_STACKS[config.OBS_COMPUTE]
ObsStack(
    app,
    f"{display}ObsLambda",
    lambda_env=obs_lambda_env,
    config=stack_config,
    ingest_name=config.NAME,
    env=env,
)

MetaStack = _META_STACKS[config.META_COMPUTE]
MetaStack(
    app,
    f"{display}MetaLambda",
    lambda_env=meta_lambda_env,
    config=stack_config,
    ingest_name=config.NAME,
    env=env,
)

# DEPLOYMENT
# BUILDX_NO_DEFAULT_ATTESTATIONS=1 cdk deploy TaiwanObsLambda --profile ingest
# BUILDX_NO_DEFAULT_ATTESTATIONS=1 cdk deploy TaiwanMetaLambda --profile ingest

app.synth()

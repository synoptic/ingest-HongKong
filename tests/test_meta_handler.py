"""
test_meta_handler.py — Run the Hong Kong metadata ingest locally.

Usage: python test_meta_handler.py --local|--dev|--prod
"""
import os, sys, json, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import INGEST_S3_BUCKET, NAME

_ap = argparse.ArgumentParser(description="Run Hong Kong metadata ingest locally")
_mode = _ap.add_mutually_exclusive_group(required=True)
_mode.add_argument("--local", action="store_true")
_mode.add_argument("--dev", action="store_true")
_mode.add_argument("--prod", action="store_true")
_test_flags, _ = _ap.parse_known_args()

os.environ["AWS_PROFILE"] = "ingest"
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["INTERNAL_BUCKET_NAME"] = "synoptic-ingest-hongkong-hko726"
os.environ["AWS_REGION"] = "us-west-2"

if _test_flags.prod:
    os.environ["MODE"] = "prod"
    os.environ["METAMOTH_DB_HOST"] = "localhost"
elif _test_flags.dev:
    os.environ["MODE"] = "dev"
    os.environ["METAMOTH_DB_HOST"] = "localhost"
else:
    os.environ["MODE"] = "local"

from handlers.meta_handler import HongKongMeta, lambda_handler

if __name__ == "__main__":
    if _test_flags.prod:
        fake_event = {"source": "aws.events", "detail-type": "Scheduled Event"}
        fake_context = type("FakeContext", (), {
            "function_name": f"{NAME}-meta-dev",
            "memory_limit_in_mb": 256,
            "invoked_function_arn": f"arn:aws:lambda:us-west-2:123456789:function:{NAME}-meta-dev",
            "get_remaining_time_in_millis": lambda self: 300_000,
        })()
        print("=" * 60)
        print("SIMULATING LAMBDA INVOCATION")
        print("=" * 60)
        response = lambda_handler(fake_event, fake_context)
        print(f"\nLambda response: {json.dumps(response, indent=2)}")
    else:
        result = HongKongMeta().run()
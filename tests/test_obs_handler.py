
"""
test_obs_handler.py — Run the Taiwan obs ingest locally.

Usage: python test_obs_handler.py [--full] [--lambda]
"""
import os, sys, json, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import INGEST_S3_BUCKET, NAME

_ap = argparse.ArgumentParser(add_help=False)
_ap.add_argument("--full", action="store_true")
_ap.add_argument("--lambda", dest="as_lambda", action="store_true")
_test_flags, _ = _ap.parse_known_args()

os.environ["AWS_PROFILE"] = "ingest"
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["INTERNAL_BUCKET_NAME"] = INGEST_S3_BUCKET
os.environ["AWS_REGION"] = "us-west-2"
os.environ.setdefault("POE_SOCKET_ADDRESS", "localhost")
os.environ.setdefault("POE_SOCKET_PORT", "18095")

if _test_flags.as_lambda:
    os.environ["MODE"] = "prod"
elif _test_flags.full:
    os.environ["MODE"] = "dev"
else:
    os.environ["MODE"] = "local"

from handlers.obs_handler import HongKongIngest, lambda_handler

if __name__ == "__main__":
    if _test_flags.as_lambda:
        fake_event = {"source": "aws.events", "detail-type": "Scheduled Event"}
        fake_context = type("FakeContext", (), {
            "function_name": f"{NAME}-obs-dev",
            "memory_limit_in_mb": 256,
            "invoked_function_arn": f"arn:aws:lambda:us-west-2:123456789:function:{NAME}-obs-dev",
            "get_remaining_time_in_millis": lambda self: 300_000,
        })()
        print("=" * 60)
        print("SIMULATING LAMBDA INVOCATION")
        print("=" * 60)
        response = lambda_handler(fake_event, fake_context)
        print(f"\nLambda response: {json.dumps(response, indent=2)}")
    else:
        result = HongKongIngest().run()
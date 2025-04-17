import argparse
import os

import pulsar
from billing_messager import ResourceUsageMessager
from eodhp_utils.pulsar.messages import generate_billingevent_schema
from eodhp_utils.runner import setup_logging
from utils import parse_iso_timestamp

setup_logging(verbosity=1, enable_otel_logging=True)


PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
PULSAR_SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
PULSAR_TOPIC = os.getenv("PULSAR_TOPIC", "billing-events")

client = pulsar.Client(PULSAR_SERVICE_URL)
producer = client.create_producer(topic=PULSAR_TOPIC, schema=generate_billingevent_schema())

parser = argparse.ArgumentParser()
parser.add_argument("--from", dest="from_time", help="ISO8601 timestamp to start from")
args = parser.parse_args()

messager = ResourceUsageMessager(
    prometheus_url=PROMETHEUS_URL,
    producer=producer,
    start_time=parse_iso_timestamp(args.from_time) if args.from_time else None,
    explicit_start=bool(args.from_time),
)

if __name__ == "__main__":
    try:
        messager.run_periodic()
    except KeyboardInterrupt:
        print("Stopping service.")
    finally:
        client.close()

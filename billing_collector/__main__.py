import logging
import os

import click
import pulsar
from eodhp_utils.pulsar.messages import generate_billingevent_schema
from eodhp_utils.runner import log_component_version, setup_logging

from .billing_messager import ResourceUsageMessager
from .utils import parse_iso_timestamp

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
PULSAR_SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
PULSAR_TOPIC = os.getenv("PULSAR_TOPIC", "billing-events")


@click.command()
@click.option("-v", "--verbose", count=True, help="Increase verbosity level.")
@click.option("--pulsar-url", default=PULSAR_SERVICE_URL, help="URL for Pulsar service.")
@click.option("--from", "from_time", help="ISO8601 timestamp to start from.")
def cli(verbose: int, pulsar_url: str, from_time: str):
    setup_logging(verbosity=verbose, enable_otel_logging=True)
    log_component_version("billing-collector")

    logging.info("Starting resource usage messager.")

    client = pulsar.Client(pulsar_url)
    producer = client.create_producer(topic=PULSAR_TOPIC, schema=generate_billingevent_schema())

    messager = ResourceUsageMessager(
        prometheus_url=PROMETHEUS_URL,
        producer=producer,
        start_time=parse_iso_timestamp(from_time) if from_time else None,
        explicit_start=bool(from_time),
    )

    try:
        messager.run_periodic()
    except KeyboardInterrupt:
        logging.info("Stopping service.")
    finally:
        client.close()


if __name__ == "__main__":
    cli()

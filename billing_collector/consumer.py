# for local testing

import logging

import pulsar
from eodhp_utils.pulsar.messages import generate_billingevent_schema

PROMETHEUS_URL = "http://localhost:9090"
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
PULSAR_TOPIC = "billing-events"

client = pulsar.Client(PULSAR_SERVICE_URL)

consumer = client.subscribe(
    topic=PULSAR_TOPIC,
    schema=generate_billingevent_schema(),
    subscription_name="billing-events-subscription",
)

logging.info("Waiting for messages...")

try:
    while True:
        msg = consumer.receive()
        try:
            billing_event = msg.value()
            logging.info(billing_event)
            consumer.acknowledge(msg)
        except Exception as e:
            logging.error("Failed processing message:", e)
            consumer.negative_acknowledge(msg)
except KeyboardInterrupt:
    logging.warning("Interrupted by user. Exiting...")
finally:
    client.close()

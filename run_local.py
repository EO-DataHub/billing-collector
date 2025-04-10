import pulsar
from billing_messager import ResourceUsageMessager
from eodhp_utils.pulsar.messages import generate_billingevent_schema

PROMETHEUS_URL = "http://localhost:9090"
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
PULSAR_TOPIC = "billing-events"

client = pulsar.Client(PULSAR_SERVICE_URL)

producer = client.create_producer(
    topic=PULSAR_TOPIC,
    schema=generate_billingevent_schema()
)

messager = ResourceUsageMessager(
    prometheus_url=PROMETHEUS_URL,
    producer=producer
)

if __name__ == "__main__":
    try:
        messager.run_periodic()
    except KeyboardInterrupt:
        print("Stopping service.")
    finally:
        client.close()
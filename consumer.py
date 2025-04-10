import pulsar
from eodhp_utils.pulsar.messages import BillingEvent, generate_billingevent_schema

PROMETHEUS_URL = "http://localhost:9090"
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
PULSAR_TOPIC = "billing-events"

client = pulsar.Client(PULSAR_SERVICE_URL)

consumer = client.subscribe(
    topic=PULSAR_TOPIC,
    schema=generate_billingevent_schema(),
    subscription_name="local-billing-events-subscription",
)

print("Waiting for messages...")

try:
    while True:
        msg = consumer.receive()
        try:
            billing_event = msg.value()
            print(billing_event)
            consumer.acknowledge(msg)
        except Exception as e:
            print("Failed processing message:", e)
            consumer.negative_acknowledge(msg)
except KeyboardInterrupt:
    print("Interrupted by user. Exiting...")
finally:
    client.close()

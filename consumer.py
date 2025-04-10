import pulsar
from billing_schema import BillingEvent

PULSAR_SERVICE_URL = "pulsar://localhost:6650"
PULSAR_TOPIC = "persistent://public/billing/usage"
SUBSCRIPTION_NAME = "local-subscription"

client = pulsar.Client(PULSAR_SERVICE_URL)

consumer = client.subscribe(
    topic=PULSAR_TOPIC,
    subscription_name=SUBSCRIPTION_NAME,
    schema=pulsar.schema.JsonSchema(BillingEvent),
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

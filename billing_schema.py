from pulsar.schema import Record, String, Float


class BillingEvent(Record):
    namespace = String()
    cpu_seconds_total = Float()
    memory_gb_seconds_total = Float()
    timestamp = String()
    source_run_id = String()

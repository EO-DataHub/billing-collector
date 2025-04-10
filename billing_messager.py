import json
import time
import uuid
import requests
from typing import Sequence
from eodhp_utils.messagers import PulsarJSONMessager, Messager
from billing_schema import BillingEvent


class ResourceUsageMessager(PulsarJSONMessager[BillingEvent]):
    def __init__(self, prometheus_url: str, **kwargs):
        super().__init__(**kwargs)
        self.prometheus_url = prometheus_url
        self.cumulative_usage = {}
        self.run_id = str(uuid.uuid4())
        self.scrape_interval_sec = 300  # 5 mins interval

    def query_prometheus(self, query: str):
        resp = requests.get(
            f"{self.prometheus_url}/api/v1/query", params={"query": query}
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("result", [])

    def collect_usage(self):
        cpu_query = f'sum(increase(container_cpu_usage_seconds_total{{namespace=~"ws-.*"}}[{self.scrape_interval_sec}s])) by (namespace)'
        mem_query = f'sum(avg_over_time(container_memory_usage_bytes{{namespace=~"ws-.*"}}[{self.scrape_interval_sec}s])) by (namespace)'

        cpu_data = self.query_prometheus(cpu_query)
        mem_data = self.query_prometheus(mem_query)

        usage_this_interval = {}
        for entry in cpu_data:
            ns = entry["metric"]["namespace"]
            usage_this_interval.setdefault(ns, {})["cpu"] = float(entry["value"][1])

        for entry in mem_data:
            ns = entry["metric"]["namespace"]
            avg_bytes = float(entry["value"][1])
            mem_gb_seconds = (avg_bytes * self.scrape_interval_sec) / (1024**3)
            usage_this_interval.setdefault(ns, {})["mem"] = mem_gb_seconds

        return usage_this_interval

    def process_payload(self, _: BillingEvent) -> Sequence[Messager.Action]:
        return []

    def gen_empty_catalogue_message(self, msg):
        raise NotImplementedError("Billing service doesn't produce catalogue messages.")

    def run_periodic(self):
        while True:
            usage = self.collect_usage()
            timestamp_iso = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time())
            )

            for ns, data in usage.items():
                prev = self.cumulative_usage.get(ns, {"cpu": 0.0, "mem": 0.0})
                cumulative_cpu = prev["cpu"] + data.get("cpu", 0.0)
                cumulative_mem = prev["mem"] + data.get("mem", 0.0)

                self.cumulative_usage[ns] = {
                    "cpu": cumulative_cpu,
                    "mem": cumulative_mem,
                }

                event = BillingEvent(
                    namespace=ns,
                    cpu_seconds_total=round(cumulative_cpu, 4),
                    memory_gb_seconds_total=round(cumulative_mem, 4),
                    timestamp=timestamp_iso,
                    source_run_id=self.run_id,
                )

                properties = {}
                self.producer.send(event, properties=properties)

                print(f"Sent billing event for namespace {ns} at {timestamp_iso}")

            time.sleep(self.scrape_interval_sec)

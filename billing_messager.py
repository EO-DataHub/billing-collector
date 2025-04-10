import os
import time
import uuid
import requests
from datetime import datetime, timedelta
from typing import Sequence

from eodhp_utils.messagers import PulsarJSONMessager, Messager
from eodhp_utils.pulsar.messages import BillingEvent


WORKSPACE_NAMESPACE_PREFIX = os.getenv("WORKSPACE_NAMESPACE_PREFIX", "ws-")
SCRAPE_INTERVAL_SEC = int(os.getenv("SCRAPE_INTERVAL_SEC", 300))  # 5 mins interval


class ResourceUsageMessager(PulsarJSONMessager[BillingEvent]):
    def __init__(self, prometheus_url: str, **kwargs):
        super().__init__(**kwargs)
        self.prometheus_url = prometheus_url
        self.scrape_interval_sec = SCRAPE_INTERVAL_SEC

    def query_prometheus(self, query: str):
        resp = requests.get(
            f"{self.prometheus_url}/api/v1/query", params={"query": query}
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("result", [])

    def collect_usage(self, start_time: datetime, end_time: datetime):
        interval_sec = int((end_time - start_time).total_seconds())

        cpu_query = f'sum(increase(container_cpu_usage_seconds_total{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])) by (namespace)'
        mem_query = f'sum(avg_over_time(container_memory_usage_bytes{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])) by (namespace)'

        cpu_data = self.query_prometheus(cpu_query)
        mem_data = self.query_prometheus(mem_query)

        usage = {}
        for entry in cpu_data:
            ns = entry["metric"]["namespace"]
            usage.setdefault(ns, {})["cpu"] = float(entry["value"][1])

        for entry in mem_data:
            ns = entry["metric"]["namespace"]
            avg_bytes = float(entry["value"][1])
            mem_gb_seconds = (avg_bytes * interval_sec) / (1024**3)
            usage.setdefault(ns, {})["mem"] = mem_gb_seconds

        return usage

    def send_event(self, workspace, sku, quantity, start, end):
        event = BillingEvent(
            uuid=str(uuid.uuid4()),
            event_start=start.isoformat() + "Z",
            event_end=end.isoformat() + "Z",
            sku=sku,
            user=None,  # Currently not tracked
            workspace=workspace,
            quantity=round(quantity, 6),
        )
        self.producer.send(event)
        print(
            f"Sent {sku} billing event for {workspace} ({quantity}) [{start} - {end}]"
        )

    def process_payload(self, _: BillingEvent) -> Sequence[Messager.Action]:
        return []

    def run_periodic(self):
        while True:
            event_end = datetime.utcnow()
            event_start = event_end - timedelta(seconds=self.scrape_interval_sec)

            usage = self.collect_usage(event_start, event_end)

            for workspace, data in usage.items():
                if "cpu" in data:
                    self.send_event(
                        workspace, "cpu-seconds", data["cpu"], event_start, event_end
                    )
                if "mem" in data:
                    self.send_event(
                        workspace,
                        "memory-gb-seconds",
                        data["mem"],
                        event_start,
                        event_end,
                    )

            sleep_time = max(
                0,
                self.scrape_interval_sec
                - (datetime.utcnow() - event_end).total_seconds(),
            )
            time.sleep(sleep_time)

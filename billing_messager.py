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

        # CPU usage: total CPU-seconds (actual usage)
        cpu_query = f'''
        sum by (namespace)(
            increase(container_cpu_usage_seconds_total{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])
        )
        '''

        # Memory usage: average memory usage in GB-seconds (actual usage)
        mem_query = f'''
        sum by (namespace)(
            avg_over_time(container_memory_usage_bytes{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])
        )
        '''

        # Requested CPU: average CPU requested in CPU-seconds
        requested_cpu_query = f'''
        sum by (namespace)(
            avg_over_time(kube_pod_container_resource_requests{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*", resource="cpu"}}[{interval_sec}s])
            * on(namespace, pod) group_left()
            (kube_pod_status_phase{{phase="Running"}} == 1)
        )
        '''

        # Requested Memory: average memory requested in bytes (to be converted to GB-seconds)
        requested_mem_query = f'''
        sum by (namespace)(
            avg_over_time(kube_pod_container_resource_requests{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*", resource="memory"}}[{interval_sec}s])
            * on(namespace, pod) group_left()
            (kube_pod_status_phase{{phase="Running"}} == 1)
        )
        '''

        cpu_data = self.query_prometheus(cpu_query)
        mem_data = self.query_prometheus(mem_query)
        req_cpu_data = self.query_prometheus(requested_cpu_query)
        req_mem_data = self.query_prometheus(requested_mem_query)

        usage = {}

        for entry in cpu_data:
            ns = entry["metric"]["namespace"]
            usage.setdefault(ns, {})["cpu"] = float(entry["value"][1])

        for entry in mem_data:
            ns = entry["metric"]["namespace"]
            avg_bytes = float(entry["value"][1])
            # Convert average bytes to GB-seconds
            usage.setdefault(ns, {})["mem"] = (avg_bytes * interval_sec) / (1024**3)

        for entry in req_cpu_data:
            ns = entry["metric"]["namespace"]
            avg_cpu_cores = float(entry["value"][1])
            # Convert average CPU cores to CPU-seconds
            usage.setdefault(ns, {})["requested_cpu"] = (avg_cpu_cores * interval_sec)

        for entry in req_mem_data:
            ns = entry["metric"]["namespace"]
            avg_requested_bytes = float(entry["value"][1])
            # Convert average bytes to GB-seconds
            usage.setdefault(ns, {})["requested_mem"] = (avg_requested_bytes * interval_sec) / (1024**3)

        return usage

    def send_event(self, workspace, sku, quantity, start, end):
        event = BillingEvent(
            uuid=str(uuid.uuid4()),
            event_start=start.isoformat() + "Z",
            event_end=end.isoformat() + "Z",
            sku=sku,
            user=None,
            workspace=workspace,
            quantity=round(quantity, 6),
        )
        self.producer.send(event)

    def process_payload(self, _: BillingEvent) -> Sequence[Messager.Action]:
        return []

    def run_periodic(self):
        while True:
            event_end = datetime.utcnow()
            event_start = event_end - timedelta(seconds=self.scrape_interval_sec)

            usage = self.collect_usage(event_start, event_end)

            for workspace, data in usage.items():

                print(f"Workspace: {workspace}, Data: {data}")

                cpu_to_bill = max(data.get("cpu", 0), data.get("requested_cpu", 0))
                mem_to_bill = max(data.get("mem", 0), data.get("requested_mem", 0))

                if cpu_to_bill:
                    self.send_event(workspace, "cpu-seconds", cpu_to_bill, event_start, event_end)
                if mem_to_bill:
                    self.send_event(workspace, "memory-gb-seconds", mem_to_bill, event_start, event_end)

            sleep_time = max(
                0,
                self.scrape_interval_sec - (datetime.utcnow() - event_end).total_seconds(),
            )
            time.sleep(sleep_time)
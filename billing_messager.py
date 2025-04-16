import os
import time
import uuid
import requests
from datetime import datetime, timedelta
from typing import Sequence
from eodhp_utils.messagers import PulsarJSONMessager, Messager
from eodhp_utils.pulsar.messages import BillingEvent
from opentelemetry import trace, baggage
from opentelemetry.trace import SpanKind
from opentelemetry.propagate import inject

WORKSPACE_NAMESPACE_PREFIX = os.getenv("WORKSPACE_NAMESPACE_PREFIX", "ws-")
SCRAPE_INTERVAL_SEC = 300  # 5 minutes fixed
DATA_COMPLETENESS_DELAY_SEC = 60  # 1 minute delay for Prometheus data completeness

class ResourceUsageMessager(PulsarJSONMessager[BillingEvent]):
    def __init__(self, prometheus_url: str, start_time: datetime = None, **kwargs):
        super().__init__(**kwargs)
        self.prometheus_url = prometheus_url
        self.scrape_interval_sec = SCRAPE_INTERVAL_SEC
        self.start_time = start_time or (datetime.utcnow() - timedelta(hours=1))

    def query_prometheus(self, query: str):
        resp = requests.get(f"{self.prometheus_url}/api/v1/query", params={"query": query})
        resp.raise_for_status()
        return resp.json().get("data", {}).get("result", [])
    
    def query_prometheus_range(self, query: str, start: datetime, end: datetime, step: int):
        resp = requests.get(
            f"{self.prometheus_url}/api/v1/query_range",
            params={
                "query": query,
                "start": start.timestamp(),
                "end": end.timestamp(),
                "step": step
            }
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("result", [])

    def collect_usage(self, start_time: datetime, end_time: datetime):
        interval_sec = int((end_time - start_time).total_seconds())
        
        queries = {
            "cpu": f'''
                sum by (namespace)(
                    increase(container_cpu_usage_seconds_total{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])
                )
            ''',
            "mem": f'''
                sum by (namespace)(
                    avg_over_time(container_memory_usage_bytes{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])
                )
            ''',
            "requested_cpu": f'''
                sum by (namespace)(
                    avg_over_time(kube_pod_container_resource_requests{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*", resource="cpu"}}[{interval_sec}s])
                    * on(namespace, pod) group_left()
                    (kube_pod_status_phase{{phase="Running"}} == 1)
                )
            ''',
            "requested_mem": f'''
                sum by (namespace)(
                    avg_over_time(kube_pod_container_resource_requests{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*", resource="memory"}}[{interval_sec}s])
                    * on(namespace, pod) group_left()
                    (kube_pod_status_phase{{phase="Running"}} == 1)
                )
            '''
        }

        usage = {}
        for key, query in queries.items():
            results = self.query_prometheus_range(query, start_time, end_time, interval_sec)

            for entry in results:
                ns = entry["metric"]["namespace"]
                values = entry["values"]

                if values:
                    value = float(values[-1][1])
                else:
                    value = 0.0

                if key in ["mem", "requested_mem"]:
                    # Convert bytes to GB-seconds
                    value = (value * interval_sec) / (1024**3)
                elif key in ["requested_cpu"]:
                    value = value * interval_sec

                usage.setdefault(ns, {})[key] = value

        return usage

    def send_event(self, workspace, sku, quantity, start, end):
        # Generate deterministic UUID
        event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{workspace}-{sku}-{start.isoformat()}")

        event = BillingEvent(
            uuid=str(event_uuid),
            event_start=start.isoformat() + "Z",
            event_end=end.isoformat() + "Z",
            sku=sku,
            user=None,
            workspace=workspace,
            quantity=round(quantity, 6),
        )

        # OTEL context injection
        properties = {}
        inject(properties)

        self.producer.send(event, properties=properties)

    def process_payload(self, _: BillingEvent) -> Sequence[Messager.Action]:
        return []

    def run_periodic(self):
        tracer = trace.get_tracer(__name__)
        next_run_time = self.start_time.replace(second=0, microsecond=0)
        next_run_time -= timedelta(minutes=next_run_time.minute % 5)

        while True:
            current_time = datetime.utcnow() - timedelta(seconds=DATA_COMPLETENESS_DELAY_SEC)
            if next_run_time + timedelta(seconds=self.scrape_interval_sec) > current_time:
                sleep_duration = (next_run_time + timedelta(seconds=self.scrape_interval_sec) - current_time).total_seconds()
                time.sleep(sleep_duration)
                continue

            interval_end = next_run_time + timedelta(seconds=self.scrape_interval_sec)

            usage = self.collect_usage(next_run_time, interval_end)

            for workspace, data in usage.items():
                with tracer.start_as_current_span(
                    "process_workspace",
                    kind=SpanKind.INTERNAL,
                    attributes={"workspace": workspace},
                ) as span:
                    baggage.set_baggage("workspace", workspace)
                    
                    cpu_to_bill = max(data.get("cpu", 0), data.get("requested_cpu", 0))
                    mem_to_bill = max(data.get("mem", 0), data.get("requested_mem", 0))

                    if cpu_to_bill:
                        self.send_event(workspace, "cpu-seconds", cpu_to_bill, next_run_time, interval_end)
                    if mem_to_bill:
                        self.send_event(workspace, "memory-gb-seconds", mem_to_bill, next_run_time, interval_end)

            next_run_time += timedelta(seconds=self.scrape_interval_sec)

            # If running as a recovery job, exit when caught up
            if self.start_time and next_run_time >= datetime.utcnow() - timedelta(seconds=DATA_COMPLETENESS_DELAY_SEC):
                break
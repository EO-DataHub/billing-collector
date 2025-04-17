import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Sequence

import requests
from eodhp_utils.messagers import Messager, PulsarJSONMessager
from eodhp_utils.pulsar.messages import BillingEvent
from opentelemetry import baggage, trace
from opentelemetry.context import attach, detach

from .utils import bytes_avg_to_gb_seconds, parse_workspace_name

WORKSPACE_NAMESPACE_PREFIX = os.getenv("WORKSPACE_NAMESPACE_PREFIX", "ws-")
SCRAPE_INTERVAL_SEC = int(os.getenv("SCRAPE_INTERVAL_SEC", "300"))
DATA_COMPLETENESS_DELAY_SEC = int(os.getenv("DATA_COMPLETENESS_DELAY_SEC", "60"))

tracer = trace.get_tracer("billing-collector")


class ResourceUsageMessager(PulsarJSONMessager[BillingEvent, BillingEvent]):
    def __init__(
        self,
        prometheus_url: str,
        start_time: datetime = None,
        explicit_start: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.prometheus_url = prometheus_url
        self.scrape_interval_sec = SCRAPE_INTERVAL_SEC
        self.start_time = start_time or (datetime.now(UTC) - timedelta(hours=1))
        self.explicit_start = explicit_start

    def query_prometheus_range(self, query: str, start: datetime, end: datetime, step: int):
        """
        Query Prometheus for a range of data including historical.
        """
        resp = requests.get(
            f"{self.prometheus_url}/api/v1/query_range",
            params={
                "query": query,
                "start": start.timestamp(),
                "end": end.timestamp(),
                "step": step,
            },
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("result", [])

    def collect_usage(self, start_time: datetime, end_time: datetime):
        """
        Send the Prometheus query to get the usage data for the given time range for CPU and memory.
        """
        interval_sec = int((end_time - start_time).total_seconds())

        queries = {
            "cpu": f"""
                sum by (namespace)(
                    increase(container_cpu_usage_seconds_total{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])
                )
            """,
            "mem": f"""
                sum by (namespace)(
                    avg_over_time(container_memory_usage_bytes{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*"}}[{interval_sec}s])
                )
            """,
            "requested_cpu": f"""
                sum by (namespace)(
                    avg_over_time(kube_pod_container_resource_requests{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*",
                    resource="cpu"}}[{interval_sec}s])
                    * on(namespace, pod) group_left()
                    (kube_pod_status_phase{{phase="Running"}} == 1)
                )
            """,
            "requested_mem": f"""
                sum by (namespace)(
                    avg_over_time(kube_pod_container_resource_requests{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*",
                    resource="memory"}}[{interval_sec}s])
                    * on(namespace, pod) group_left()
                    (kube_pod_status_phase{{phase="Running"}} == 1)
                )
            """,
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
                    value = bytes_avg_to_gb_seconds(value, interval_sec)
                elif key in ["requested_cpu"]:
                    value = value * interval_sec

                usage.setdefault(ns, {})[key] = value

        return usage

    def send_event(self, workspace, sku, quantity, start, end):
        event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{workspace}-{sku}-{start.isoformat()}")
        event = BillingEvent(
            uuid=str(event_uuid),
            event_start=start.isoformat() + "Z",
            event_end=end.isoformat() + "Z",
            sku=sku,
            user=None,
            workspace=parse_workspace_name(workspace),
            quantity=round(quantity, 6),
        )
        return Messager.PulsarMessageAction(payload=event)

    def process_payload(self, _: BillingEvent) -> Sequence[Messager.Action]:
        return []

    def run_periodic(self):
        """
        Run the billing messager periodically, depending on the scrape interval.
        """
        next_run_time = self.start_time.replace(microsecond=0)

        while True:
            current_time = datetime.utcnow() - timedelta(seconds=DATA_COMPLETENESS_DELAY_SEC)
            if next_run_time + timedelta(seconds=self.scrape_interval_sec) > current_time:
                # Calculate the sleep duration until the next run time
                sleep_duration = (
                    next_run_time + timedelta(seconds=self.scrape_interval_sec) - current_time
                ).total_seconds()

                # Sleep until the next run time
                time.sleep(max(sleep_duration, 0))
                continue

            interval_end = next_run_time + timedelta(seconds=self.scrape_interval_sec)

            usage = self.collect_usage(next_run_time, interval_end)

            actions: list[Messager.Action] = []
            for workspace, data in usage.items():
                cpu_to_bill = max(data.get("cpu", 0), data.get("requested_cpu", 0))
                mem_to_bill = max(data.get("mem", 0), data.get("requested_mem", 0))

                if cpu_to_bill:
                    actions.append(
                        self.send_event(
                            workspace, "cpu-seconds", cpu_to_bill, next_run_time, interval_end
                        )
                    )
                if mem_to_bill:
                    actions.append(
                        self.send_event(
                            workspace, "memory-gb-seconds", mem_to_bill, next_run_time, interval_end
                        )
                    )

            for action in actions:
                with tracer.start_as_current_span(
                    "send_billing_event",
                    attributes={"workspace": action.payload.workspace, "sku": action.payload.sku},
                ):
                    token = attach(baggage.set_baggage("workspace", action.payload.workspace))
                    try:
                        self._runaction(action, Messager.CatalogueChanges(), Messager.Failures())
                    finally:
                        detach(token)

            next_run_time += timedelta(seconds=self.scrape_interval_sec)

            # If running as a recovery job, exit when caught up
            if self.explicit_start and next_run_time >= datetime.utcnow() - timedelta(
                seconds=DATA_COMPLETENESS_DELAY_SEC
            ):
                break

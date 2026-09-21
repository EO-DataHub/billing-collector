import contextlib
import logging
import os
import time
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, cast

import pulsar
import pulsar.exceptions
import requests
from botocore.client import BaseClient
from eodhp_utils.messagers import Messager, PulsarJSONMessager
from eodhp_utils.pulsar.messages import BillingEvent
from opentelemetry import baggage, trace
from opentelemetry.context import attach, detach

from .state import CheckpointState
from .utils import align_time, bytes_avg_to_gb_seconds, parse_iso_timestamp, parse_workspace_name

WORKSPACE_NAMESPACE_PREFIX = os.getenv("WORKSPACE_NAMESPACE_PREFIX", "ws-")
SCRAPE_INTERVAL_SEC = int(os.getenv("SCRAPE_INTERVAL_SEC", "300"))
DATA_COMPLETENESS_DELAY_SEC = int(os.getenv("DATA_COMPLETENESS_DELAY_SEC", "60"))
PULSAR_SEND_RETRY_ATTEMPTS = int(os.getenv("PULSAR_SEND_RETRY_ATTEMPTS", "5"))
PULSAR_SEND_RETRY_BACKOFF_SEC = float(os.getenv("PULSAR_SEND_RETRY_BACKOFF_SEC", "2"))

tracer = trace.get_tracer("billing-collector")


class ResourceUsageMessager(PulsarJSONMessager[BillingEvent, BillingEvent]):
    def __init__(
        self,
        prometheus_url: str,
        start_time: datetime | None = None,
        explicit_start: bool = False,
        producer: pulsar.Producer | None = None,
        s3_client: BaseClient | None = None,
        output_bucket: str | None = None,
        cat_output_prefix: str = "",
        state_file: str | None = None,
    ) -> None:
        super().__init__(
            producer=producer,
            s3_client=s3_client,
            output_bucket=output_bucket,
            cat_output_prefix=cat_output_prefix,
        )
        self.prometheus_url = prometheus_url
        self.scrape_interval_sec = SCRAPE_INTERVAL_SEC
        self.start_time = start_time or (datetime.utcnow() - timedelta(hours=1))
        self.explicit_start = explicit_start
        self.state_file = state_file

    def query_prometheus_range(self, query: str, start: datetime, end: datetime, step: int) -> list[dict[str, Any]]:
        """
        Query Prometheus for a range of data including historical.
        """
        print(f"Querying Prometheus: from {start} to {end} with step {step}")
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

    def collect_usage(self, start_time: datetime, end_time: datetime) -> dict[str, dict[str, float]]:
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
            "requested_gpu": f"""
                sum by (namespace)(
                    avg_over_time(kube_pod_container_resource_requests{{namespace=~"{WORKSPACE_NAMESPACE_PREFIX}.*",
                    resource="nvidia_com_gpu"}}[{interval_sec}s])
                    * on(namespace, pod) group_left()
                    (kube_pod_status_phase{{phase="Running"}} == 1)
                )
            """,
        }

        usage: dict[str, dict[str, float]] = {}
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
                elif key in ["requested_cpu", "requested_gpu"]:
                    value = value * interval_sec

                usage.setdefault(ns, {})[key] = value

        return usage

    def send_event(
        self, workspace: str, sku: str, quantity: float, start: datetime, end: datetime
    ) -> Messager.PulsarMessageAction:
        workspace = parse_workspace_name(workspace)
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
        return Messager.PulsarMessageAction(payload=cast(Any, event))

    def process_payload(self, obj: BillingEvent) -> Sequence[Messager.Action]:
        return []

    def _send_with_retry(self, action: Messager.PulsarMessageAction) -> None:
        """
        Send a single action, retrying transient Pulsar failures with backoff instead of letting
        them crash run_periodic and discard the rest of the current window's progress.
        """
        for attempt in range(1, PULSAR_SEND_RETRY_ATTEMPTS + 1):
            try:
                self._runaction(action, Messager.CatalogueChanges(), Messager.Failures())
                return
            except pulsar.exceptions.PulsarException:
                if attempt == PULSAR_SEND_RETRY_ATTEMPTS:
                    raise
                backoff = PULSAR_SEND_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                logging.warning(
                    "Pulsar send failed (attempt %d/%d), retrying in %.1fs",
                    attempt,
                    PULSAR_SEND_RETRY_ATTEMPTS,
                    backoff,
                    exc_info=True,
                )
                time.sleep(backoff)

    def run_periodic(self) -> None:
        """
        Run the billing messager periodically aligned to nice intervals (e.g. 00:00, 00:05, 00:10...).
        """
        with contextlib.ExitStack() as stack:
            state = stack.enter_context(CheckpointState(self.state_file)) if self.state_file else None

            start_time = self.start_time
            if state and not self.explicit_start and state.next_run_time:
                start_time = parse_iso_timestamp(state.next_run_time)
                logging.info("Resuming from checkpoint: %s", start_time)

            next_run_time = align_time(start_time.replace(microsecond=0), self.scrape_interval_sec)

            while True:
                current_time = datetime.utcnow() - timedelta(seconds=DATA_COMPLETENESS_DELAY_SEC)
                interval_end = next_run_time + timedelta(seconds=self.scrape_interval_sec)

                if interval_end > current_time:
                    # Sleep exactly until the aligned interval is complete
                    sleep_duration = (interval_end - current_time).total_seconds()
                    time.sleep(max(sleep_duration, 0))
                    continue

                usage = self.collect_usage(next_run_time, interval_end)

                actions: list[Messager.PulsarMessageAction] = []
                for workspace, data in usage.items():
                    cpu_to_bill = max(data.get("cpu", 0), data.get("requested_cpu", 0))
                    mem_to_bill = max(data.get("mem", 0), data.get("requested_mem", 0))
                    gpu_to_bill = data.get("requested_gpu", 0)

                    if cpu_to_bill:
                        actions.append(
                            self.send_event(workspace, "cpu-seconds", cpu_to_bill, next_run_time, interval_end)
                        )
                    if mem_to_bill:
                        actions.append(
                            self.send_event(workspace, "memory-gb-seconds", mem_to_bill, next_run_time, interval_end)
                        )
                    if gpu_to_bill:
                        actions.append(
                            self.send_event(workspace, "gpu-seconds", gpu_to_bill, next_run_time, interval_end)
                        )

                for action in actions:
                    payload = cast(BillingEvent, action.payload)
                    with tracer.start_as_current_span(
                        "send_billing_event",
                        attributes={"workspace": str(payload.workspace), "sku": str(payload.sku)},
                    ):
                        token = attach(baggage.set_baggage("workspace", str(payload.workspace)))
                        try:
                            self._send_with_retry(action)
                        finally:
                            detach(token)

                next_run_time = interval_end
                if state:
                    state.mark_next_run_time(next_run_time.isoformat())

                # If running as a recovery job, exit when caught up
                if self.explicit_start and next_run_time >= datetime.utcnow() - timedelta(
                    seconds=DATA_COMPLETENESS_DELAY_SEC
                ):
                    break

import datetime as dt
from unittest import mock

import pytest

from billing_collector import billing_messager as bcm
from billing_collector.utils import (
    bytes_avg_to_gb_seconds,
    parse_iso_timestamp,
    parse_workspace_name,
)


def test_bytes_avg_to_gb_seconds():
    gib = 1024**3
    assert bytes_avg_to_gb_seconds(gib, 1) == 1
    assert bytes_avg_to_gb_seconds(gib, 600) == 600


def test_parse_iso_timestamp_ok():
    ts = "2025-04-17T10:00:00Z"
    parsed = parse_iso_timestamp(ts)
    assert parsed.isoformat() == "2025-04-17T10:00:00+00:00"


def test_parse_iso_timestamp_bad():
    with pytest.raises(ValueError):
        parse_iso_timestamp("definitely-not-an-iso-date")


def test_parse_workspace_name():
    assert parse_workspace_name("ws-foo") == "foo"
    assert parse_workspace_name("ws-bar") == "bar"
    assert parse_workspace_name("ws-") == ""
    assert parse_workspace_name("foo") == "foo"
    assert parse_workspace_name("") == ""
    assert parse_workspace_name("ws-123") == "123"


def test_collect_usage():
    mock_producer = mock.MagicMock()

    messager = bcm.ResourceUsageMessager(
        prometheus_url="http://mock-prometheus",
        producer=mock_producer,
    )

    messager.query_prometheus_range = mock.MagicMock(
        side_effect=[
            # cpu
            [{"metric": {"namespace": "ws-ns1"}, "values": [[0, "10"]]}],
            # mem
            [{"metric": {"namespace": "ws-ns1"}, "values": [[0, str(2 * 1024**3)]]}],
            # requested_cpu
            [{"metric": {"namespace": "ws-ns1"}, "values": [[0, "8"]]}],
            # requested_mem
            [{"metric": {"namespace": "ws-ns1"}, "values": [[0, str(1 * 1024**3)]]}],
        ]
    )

    start = dt.datetime(2025, 4, 17, 0, 0, 0)
    end = start + dt.timedelta(seconds=600)
    interval = int((end - start).total_seconds())

    usage = messager.collect_usage(start, end)

    # Validate the mocked calls explicitly
    assert messager.query_prometheus_range.call_count == 4

    u = usage["ws-ns1"]
    # CPU
    assert u["cpu"] == 10
    assert u["requested_cpu"] == 8 * interval  # 8 cores × 600 s
    # Memory
    assert u["mem"] == bytes_avg_to_gb_seconds(2 * 1024**3, interval)  # 2 GiB × 600 s → 1200
    assert u["requested_mem"] == bytes_avg_to_gb_seconds(
        1 * 1024**3, interval
    )  # 1 GiB × 600 s → 600

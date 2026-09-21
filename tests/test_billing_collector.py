import datetime as dt
import json
from pathlib import Path
from unittest import mock

import pulsar.exceptions
import pytest
import requests.exceptions

from billing_collector import billing_messager as bcm
from billing_collector.state import CheckpointState
from billing_collector.utils import (
    bytes_avg_to_gb_seconds,
    parse_iso_timestamp,
    parse_workspace_name,
)


def test_bytes_avg_to_gb_seconds() -> None:
    gib = 1024**3
    assert bytes_avg_to_gb_seconds(gib, 1) == 1
    assert bytes_avg_to_gb_seconds(gib, 600) == 600


def test_parse_iso_timestamp_ok() -> None:
    ts = "2025-04-17T10:00:00Z"
    parsed = parse_iso_timestamp(ts)
    assert parsed.isoformat() == "2025-04-17T10:00:00+00:00"


def test_parse_iso_timestamp_bad() -> None:
    with pytest.raises(ValueError, match="Invalid ISO8601 timestamp"):
        parse_iso_timestamp("definitely-not-an-iso-date")


def test_parse_iso_timestamp_assumes_utc_when_no_tz_given() -> None:
    parsed = parse_iso_timestamp("2025-04-17T10:00:00")
    assert parsed.isoformat() == "2025-04-17T10:00:00+00:00"


def test_parse_workspace_name() -> None:
    assert parse_workspace_name("ws-foo") == "foo"
    assert parse_workspace_name("ws-bar") == "bar"
    assert parse_workspace_name("ws-") == ""
    assert parse_workspace_name("foo") == "foo"
    assert parse_workspace_name("") == ""
    assert parse_workspace_name("ws-123") == "123"


def test_align_time_is_independent_of_input_offset() -> None:
    # Same instant expressed in two different offsets must align to the same UTC result.
    utc_dt = dt.datetime(2025, 4, 17, 10, 7, 30, tzinfo=dt.UTC)
    plus_two_dt = utc_dt.astimezone(dt.timezone(dt.timedelta(hours=2)))

    expected = dt.datetime(2025, 4, 17, 10, 5, 0, tzinfo=dt.UTC)
    assert bcm.align_time(utc_dt, 300) == expected
    assert bcm.align_time(plus_two_dt, 300) == expected


def test_collect_usage() -> None:
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
            # requested_gpu
            [{"metric": {"namespace": "ws-ns1"}, "values": [[0, "1"]]}],
        ]
    )

    start = dt.datetime(2025, 4, 17, 0, 0, 0, tzinfo=dt.UTC)
    end = start + dt.timedelta(seconds=600)
    interval = int((end - start).total_seconds())

    usage = messager.collect_usage(start, end)

    # Validate the mocked calls explicitly
    assert messager.query_prometheus_range.call_count == 5

    u = usage["ws-ns1"]
    # CPU
    assert u["cpu"] == 10
    assert u["requested_cpu"] == 8 * interval  # 8 cores x 600 s
    # Memory
    assert u["mem"] == bytes_avg_to_gb_seconds(2 * 1024**3, interval)  # 2 GiB x 600 s = 1200
    assert u["requested_mem"] == bytes_avg_to_gb_seconds(1 * 1024**3, interval)  # 1 GiB x 600 s = 600
    # GPU
    assert u["requested_gpu"] == 1 * interval  # 1 GPU x 600 s


def test_checkpoint_state_defaults_to_none(tmp_path: Path) -> None:
    with CheckpointState(str(tmp_path / "state.json")) as state:
        assert state.next_run_time is None


def test_checkpoint_state_persists_across_reopen(tmp_path: Path) -> None:
    state_file = str(tmp_path / "state.json")

    with CheckpointState(state_file) as state:
        state.mark_next_run_time("2025-04-17T10:05:00")

    with CheckpointState(state_file) as state:
        assert state.next_run_time == "2025-04-17T10:05:00"

    with open(state_file) as f:
        assert json.load(f) == {"next_run_time": "2025-04-17T10:05:00"}


def test_send_with_retry_recovers_from_transient_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_producer = mock.MagicMock()
    messager = bcm.ResourceUsageMessager(prometheus_url="http://mock-prometheus", producer=mock_producer)

    sleeps: list[float] = []
    monkeypatch.setattr(bcm.time, "sleep", lambda s: sleeps.append(s))

    calls = {"n": 0}

    def flaky_runaction(action: object, cat_changes: object, failures: object) -> None:
        calls["n"] += 1
        if calls["n"] < 3:
            raise pulsar.exceptions.Timeout("transient")

    messager._runaction = mock.MagicMock(side_effect=flaky_runaction)  # type: ignore[method-assign]

    action = messager.send_event(
        "ws-foo",
        "cpu-seconds",
        1.0,
        dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
        dt.datetime(2025, 1, 1, 0, 5, tzinfo=dt.UTC),
    )
    messager._send_with_retry(action)

    assert calls["n"] == 3
    assert len(sleeps) == 2


def test_send_with_retry_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_producer = mock.MagicMock()
    messager = bcm.ResourceUsageMessager(prometheus_url="http://mock-prometheus", producer=mock_producer)
    monkeypatch.setattr(bcm.time, "sleep", lambda s: None)

    messager._runaction = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=pulsar.exceptions.Timeout("always fails")
    )

    action = messager.send_event(
        "ws-foo",
        "cpu-seconds",
        1.0,
        dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
        dt.datetime(2025, 1, 1, 0, 5, tzinfo=dt.UTC),
    )

    with pytest.raises(pulsar.exceptions.PulsarException):
        messager._send_with_retry(action)

    assert messager._runaction.call_count == bcm.PULSAR_SEND_RETRY_ATTEMPTS


def test_run_periodic_resumes_from_checkpoint_over_default_start(tmp_path: Path) -> None:
    state_file = str(tmp_path / "state.json")
    checkpoint_time = dt.datetime(2025, 1, 1, 0, 10, 0, tzinfo=dt.UTC)
    with open(state_file, "w") as f:
        json.dump({"next_run_time": checkpoint_time.isoformat()}, f)

    mock_producer = mock.MagicMock()
    messager = bcm.ResourceUsageMessager(
        prometheus_url="http://mock-prometheus",
        producer=mock_producer,
        # Would be used if the checkpoint were ignored:
        start_time=dt.datetime(2000, 1, 1, tzinfo=dt.UTC),
        explicit_start=False,
        state_file=state_file,
    )

    class _StopTest(Exception):
        pass

    seen: list[dt.datetime] = []

    def fake_collect_usage(start_time: dt.datetime, end_time: dt.datetime) -> dict:
        seen.append(start_time)
        raise _StopTest

    messager.collect_usage = mock.MagicMock(side_effect=fake_collect_usage)  # type: ignore[method-assign]

    with pytest.raises(_StopTest):
        messager.run_periodic()

    assert seen == [bcm.align_time(checkpoint_time, messager.scrape_interval_sec)]


def test_run_periodic_explicit_start_ignores_checkpoint(tmp_path: Path) -> None:
    state_file = str(tmp_path / "state.json")
    checkpoint_time = dt.datetime(2025, 1, 1, 0, 10, 0, tzinfo=dt.UTC)
    with open(state_file, "w") as f:
        json.dump({"next_run_time": checkpoint_time.isoformat()}, f)

    explicit_start_time = dt.datetime(2024, 6, 1, 0, 0, 0, tzinfo=dt.UTC)
    mock_producer = mock.MagicMock()
    messager = bcm.ResourceUsageMessager(
        prometheus_url="http://mock-prometheus",
        producer=mock_producer,
        start_time=explicit_start_time,
        explicit_start=True,
        state_file=state_file,
    )

    class _StopTest(Exception):
        pass

    seen: list[dt.datetime] = []

    def fake_collect_usage(start_time: dt.datetime, end_time: dt.datetime) -> dict:
        seen.append(start_time)
        raise _StopTest

    messager.collect_usage = mock.MagicMock(side_effect=fake_collect_usage)  # type: ignore[method-assign]

    with pytest.raises(_StopTest):
        messager.run_periodic()

    assert seen == [bcm.align_time(explicit_start_time, messager.scrape_interval_sec)]


def test_query_prometheus_range_passes_timeout() -> None:
    mock_producer = mock.MagicMock()
    messager = bcm.ResourceUsageMessager(prometheus_url="http://mock-prometheus", producer=mock_producer)

    mock_response = mock.MagicMock()
    mock_response.json.return_value = {"data": {"result": []}}

    with mock.patch("billing_collector.billing_messager.requests.get", return_value=mock_response) as mock_get:
        messager.query_prometheus_range(
            "up", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), dt.datetime(2025, 1, 1, 0, 5, tzinfo=dt.UTC), 300
        )

    assert mock_get.call_args.kwargs["timeout"] == bcm.PROMETHEUS_REQUEST_TIMEOUT_SEC


def test_collect_usage_with_retry_recovers_from_transient_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_producer = mock.MagicMock()
    messager = bcm.ResourceUsageMessager(prometheus_url="http://mock-prometheus", producer=mock_producer)

    sleeps: list[float] = []
    monkeypatch.setattr(bcm.time, "sleep", lambda s: sleeps.append(s))

    calls = {"n": 0}

    def flaky_collect_usage(start_time: dt.datetime, end_time: dt.datetime) -> dict:
        calls["n"] += 1
        if calls["n"] < 2:
            raise requests.exceptions.Timeout("transient")
        return {"ws-ns1": {"cpu": 1.0}}

    messager.collect_usage = mock.MagicMock(side_effect=flaky_collect_usage)  # type: ignore[method-assign]

    result = messager._collect_usage_with_retry(
        dt.datetime(2025, 1, 1, tzinfo=dt.UTC), dt.datetime(2025, 1, 1, 0, 5, tzinfo=dt.UTC)
    )

    assert result == {"ws-ns1": {"cpu": 1.0}}
    assert calls["n"] == 2
    assert len(sleeps) == 1


def test_collect_usage_with_retry_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_producer = mock.MagicMock()
    messager = bcm.ResourceUsageMessager(prometheus_url="http://mock-prometheus", producer=mock_producer)
    monkeypatch.setattr(bcm.time, "sleep", lambda s: None)

    messager.collect_usage = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=requests.exceptions.ConnectionError("always fails")
    )

    with pytest.raises(requests.exceptions.RequestException):
        messager._collect_usage_with_retry(
            dt.datetime(2025, 1, 1, tzinfo=dt.UTC), dt.datetime(2025, 1, 1, 0, 5, tzinfo=dt.UTC)
        )

    assert messager.collect_usage.call_count == bcm.PROMETHEUS_QUERY_RETRY_ATTEMPTS

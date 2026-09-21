from datetime import UTC, datetime


def bytes_avg_to_gb_seconds(avg_bytes: float, interval_sec: int) -> float:
    return (avg_bytes * interval_sec) / 1024**3


def parse_iso_timestamp(iso_time: str) -> datetime:
    """
    Parse an ISO8601 timestamp string into a timezone-aware UTC datetime object.

    A string with no timezone info is assumed to already represent UTC, rather than being
    interpreted against the system's local timezone.
    """
    try:
        parsed = datetime.fromisoformat(iso_time)
    except ValueError as e:
        raise ValueError(f"Invalid ISO8601 timestamp: {iso_time}") from e

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_workspace_name(workspace: str) -> str:
    """
    Parse the workspace name to extract the actual name.
    """
    if workspace.startswith("ws-"):
        return workspace[3:]
    else:
        return workspace


def align_time(dt: datetime, interval_sec: int) -> datetime:
    """
    Aligns a given timezone-aware datetime object to the nearest lower interval, in UTC.

    dt must carry tzinfo: datetime.timestamp() interprets a naive datetime as local time, which
    would make the aligned result depend on the system's timezone rather than always meaning UTC.
    """
    timestamp = int(dt.timestamp())
    aligned_timestamp = timestamp - (timestamp % interval_sec)
    return datetime.fromtimestamp(aligned_timestamp, tz=UTC)
